package database

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mattn/go-sqlite3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// pragmaSQL holds database-level settings that persist in the file; it runs once on the writer.
const pragmaSQL = "PRAGMA journal_mode=WAL;"

// Pragmas are per connection, so they are applied from a connect hook to every pooled connection.
// temp_store=FILE keeps the walked_paths temp table out of RAM; synchronous=NORMAL is corruption-safe in WAL mode.
const (
	writerConnSQL = `
		PRAGMA foreign_keys=ON;
		PRAGMA synchronous=NORMAL;
		PRAGMA temp_store=FILE;
		PRAGMA mmap_size=268435456;
		PRAGMA cache_size=-262144;`
	readerConnSQL = `
		PRAGMA mmap_size=268435456;
		PRAGMA cache_size=-32768;`
)

const (
	writerDriver = "sqlite3_filescan_rw"
	readerDriver = "sqlite3_filescan_ro"
)

func init() {
	hook := func(query string) func(*sqlite3.SQLiteConn) error {
		return func(conn *sqlite3.SQLiteConn) error {
			_, err := conn.Exec(query, nil)
			return err
		}
	}
	sql.Register(writerDriver, &sqlite3.SQLiteDriver{ConnectHook: hook(writerConnSQL)})
	sql.Register(readerDriver, &sqlite3.SQLiteDriver{ConnectHook: hook(readerConnSQL)})
}

// New initializes a new Database instance
func New(ctx context.Context, dsn string, readonly bool) (*Database, error) {
	var lockFile *os.File
	var err error

	if !readonly {
		// Create and keep lock file open to prevent multiple writers
		lockFilePath := dsn + ".lock"
		lockFile, err = createLockFile(lockFilePath)
		if err != nil {
			return nil, fmt.Errorf("error creating lock file: %w", err)
		}
	}

	mode := "rwc"
	if readonly {
		mode = "ro"
	}

	fullDSN := dsn
	if strings.Contains(dsn, "?") {
		fullDSN += "&mode=" + mode
	} else {
		fullDSN += "?mode=" + mode
	}

	driver := writerDriver
	if readonly {
		driver = readerDriver
	}

	db, err := sql.Open(driver, fullDSN)
	if err != nil {
		if lockFile != nil {
			lockFile.Close()
		}
		return nil, err
	}

	// Writers are pinned to a single physical connection
	// walked_paths is a session-scoped TEMP TABLE, all queries against it must share the same underlying SQLite connection.
	// Readers have no TEMP TABLE, let them query in parallel.
	if readonly {
		db.SetMaxOpenConns(runtime.NumCPU())
	} else {
		db.SetMaxOpenConns(1)
	}

	// Verify the connection
	err = db.Ping()
	if err != nil {
		db.Close()
		if lockFile != nil {
			lockFile.Close()
		}
		return nil, err
	}

	// Configure connection pragmas and apply pending schema migrations (only for write mode)
	var insertFileStmt *sql.Stmt
	if !readonly {
		if _, err := db.ExecContext(ctx, pragmaSQL); err != nil {
			db.Close()
			if lockFile != nil {
				lockFile.Close()
			}
			return nil, fmt.Errorf("error setting pragmas: %w", err)
		}

		if err := runMigrations(ctx, db); err != nil {
			db.Close()
			if lockFile != nil {
				lockFile.Close()
			}
			return nil, fmt.Errorf("error applying migrations: %w", err)
		}

		// walked_paths tracks which known files were seen during the current backup walk
		if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE IF NOT EXISTS walked_paths (path TEXT PRIMARY KEY) WITHOUT ROWID`); err != nil {
			db.Close()
			if lockFile != nil {
				lockFile.Close()
			}
			return nil, fmt.Errorf("error creating walked_paths temp table: %w", err)
		}

		insertFileStmt, err = db.PrepareContext(ctx, `
			INSERT INTO files (path, version, dataset_uuid, sha256, md5, size, last_modified_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			db.Close()
			if lockFile != nil {
				lockFile.Close()
			}
			return nil, fmt.Errorf("error preparing insert statement: %w", err)
		}
	}

	// Single primary-key seek to the latest version of the path
	fileUnchangedStmt, err := db.PrepareContext(ctx, `
		SELECT version, size, last_modified_at, deleted_at IS NULL
		FROM files WHERE path = ? ORDER BY version DESC LIMIT 1`)
	if err != nil {
		db.Close()
		if lockFile != nil {
			lockFile.Close()
		}
		return nil, fmt.Errorf("error preparing file unchanged statement: %w", err)
	}

	return &Database{
		db:                db,
		fileUnchangedStmt: fileUnchangedStmt,
		insertFileStmt:    insertFileStmt,
		lockFile:          lockFile,
		readonly:          readonly,
	}, nil
}

// runMigrations applies any embedded migration files newer than the database's recorded version.
func runMigrations(ctx context.Context, db *sql.DB) error {
	// We need the schema_migrations table to exist before we can check the current version.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at INTEGER NOT NULL DEFAULT (unixepoch())
		)
	`); err != nil {
		return fmt.Errorf("error creating schema_migrations table: %w", err)
	}

	var currentVersion int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&currentVersion); err != nil {
		return fmt.Errorf("error reading current schema version: %w", err)
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("error reading migrations directory: %w", err)
	}

	type migration struct {
		version int
		name    string
	}
	var migrations []migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return fmt.Errorf("invalid migration filename %q: missing version prefix", entry.Name())
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			return fmt.Errorf("invalid migration filename %q: %w", entry.Name(), err)
		}
		migrations = append(migrations, migration{version: version, name: entry.Name()})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })

	for _, m := range migrations {
		if m.version <= currentVersion {
			continue
		}

		contents, err := fs.ReadFile(migrationsFS, "migrations/"+m.name)
		if err != nil {
			return fmt.Errorf("error reading migration %q: %w", m.name, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("error beginning transaction for migration %q: %w", m.name, err)
		}

		if _, err := tx.ExecContext(ctx, string(contents)); err != nil {
			tx.Rollback()
			return fmt.Errorf("error applying migration %q: %w", m.name, err)
		}

		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (?)`, m.version); err != nil {
			tx.Rollback()
			return fmt.Errorf("error recording migration %q: %w", m.name, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("error committing migration %q: %w", m.name, err)
		}
	}

	return nil
}

// createLockFile takes an exclusive advisory lock (flock) on path to prevent multiple writers.
// The OS releases it when the process exits, so a crash never leaves a stale lock.
func createLockFile(path string) (*os.File, error) {
	var file *os.File
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open lock file: %w", err)
		}

		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			f.Close()
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return nil, fmt.Errorf("another instance is already running (lock held: %s)", path)
			}
			return nil, fmt.Errorf("failed to lock file: %w", err)
		}

		// The previous holder unlinks the file on close; retry if we locked a stale inode
		fi, err1 := f.Stat()
		pi, err2 := os.Stat(path)
		if err1 == nil && err2 == nil && os.SameFile(fi, pi) {
			file = f
			break
		}
		f.Close()
	}

	// Write process info to the lock file for debugging
	processInfo := fmt.Sprintf("PID: %d\nStarted: %s\n", os.Getpid(), time.Now().Format(time.RFC3339))
	if err := file.Truncate(0); err == nil {
		file.WriteString(processInfo)
	}

	return file, nil
}

// database holds the database connection
type Database struct {
	db                *sql.DB
	fileUnchangedStmt *sql.Stmt
	insertFileStmt    *sql.Stmt // nil in readonly mode
	lockFile          *os.File
	readonly          bool
}

// Reindex performs a REINDEX operation on the database
func (d *Database) Reindex(ctx context.Context) error {
	_, err := d.db.ExecContext(ctx, `REINDEX`)
	if err != nil {
		return fmt.Errorf("error reindexing database: %w", err)
	}
	return nil
}

// Close closes the database connection and releases the lock
func (d *Database) Close(ctx context.Context) error {
	var walErr, stmtErr, dbErr, lockErr error

	if d.db != nil && !d.readonly {
		_, walErr = d.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);")
	}

	if d.insertFileStmt != nil {
		stmtErr = d.insertFileStmt.Close()
	}
	if d.fileUnchangedStmt != nil {
		stmtErr = errors.Join(stmtErr, d.fileUnchangedStmt.Close())
	}

	// Close database connection
	if d.db != nil {
		dbErr = d.db.Close()
	}

	// Unlink while still holding the flock; createLockFile retries if it locked a stale inode
	if d.lockFile != nil {
		if err := os.Remove(d.lockFile.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			lockErr = err
		}
		lockErr = errors.Join(lockErr, d.lockFile.Close())
	}

	// Return the first error encountered
	if walErr != nil {
		return walErr
	}
	if stmtErr != nil {
		return stmtErr
	}
	if dbErr != nil {
		return dbErr
	}
	return lockErr
}
