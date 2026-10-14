package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// InsertFileItem represents a file to be inserted into the database
type InsertFileItem struct {
	IsNew          bool   // true if no prior version of Path exists
	DatasetUUID    string // empty when the path has no 32-char hex run
	Version        int64  // latest stored version of Path + 1
	LastModifiedAt int64
	MD5            string
	Path           string
	SHA256         string
	Size           int64
}

// FileState describes how a file on disk compares to its latest stored version.
type FileState int

const (
	FileNew FileState = iota
	FileModified
	FileUnchanged
)

// GetFileState compares size and lastModifiedAt against the latest stored version of path.
// A path whose latest version is marked deleted counts as modified. It also returns the latest stored version (0 if new).
func (d *Database) GetFileState(ctx context.Context, path string, size, lastModifiedAt int64) (FileState, int64, error) {
	var latestVersion, latestSize, latestModifiedAt int64
	var live bool
	err := d.fileUnchangedStmt.QueryRowContext(ctx, path).Scan(&latestVersion, &latestSize, &latestModifiedAt, &live)
	if errors.Is(err, sql.ErrNoRows) {
		return FileNew, 0, nil
	}
	if err != nil {
		return FileNew, 0, fmt.Errorf("error checking file %q: %w", path, err)
	}
	if live && latestSize == size && latestModifiedAt == lastModifiedAt {
		return FileUnchanged, latestVersion, nil
	}
	return FileModified, latestVersion, nil
}

// InsertFiles inserts files in one transaction using each item's Version.
func (d *Database) InsertFiles(ctx context.Context, files []InsertFileItem) error {
	if len(files) == 0 {
		return nil
	}
	if d.readonly {
		return fmt.Errorf("cannot insert files: database is read-only")
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	datasetStmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO datasets (uuid) VALUES (?)`)
	if err != nil {
		return fmt.Errorf("error preparing dataset insert statement: %w", err)
	}
	defer datasetStmt.Close()

	stmt := tx.StmtContext(ctx, d.insertFileStmt)
	defer stmt.Close()

	datasets := make(map[string]struct{}, len(files))
	for _, f := range files {
		if _, ok := datasets[f.DatasetUUID]; !ok {
			if _, err := datasetStmt.ExecContext(ctx, f.DatasetUUID); err != nil {
				return fmt.Errorf("error inserting dataset %q: %w", f.DatasetUUID, err)
			}
			datasets[f.DatasetUUID] = struct{}{}
		}

		if _, err := stmt.ExecContext(ctx, f.Path, f.Version, f.DatasetUUID, f.SHA256, f.MD5, f.Size, f.LastModifiedAt); err != nil {
			return fmt.Errorf("error inserting file %q: %w", f.Path, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}
	return nil
}

// MarkFilesDeleted marks each given path as deleted by timestamping deleted_at on its latest row
func (d *Database) MarkFilesDeleted(ctx context.Context, paths []string, deletedAt int64) error {
	if len(paths) == 0 {
		return nil
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `
		UPDATE files SET deleted_at = ?1
		WHERE path = ?2
			AND version = (SELECT MAX(version) FROM files WHERE path = ?2)
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, path := range paths {
		if _, err := stmt.ExecContext(ctx, deletedAt, path); err != nil {
			tx.Rollback()
			return fmt.Errorf("error marking file deleted (%s): %w", path, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// InsertWalkedPaths records paths seen during the current backup walk in the walked_paths temp table
func (d *Database) InsertWalkedPaths(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO walked_paths (path) VALUES (?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, path := range paths {
		if _, err := stmt.ExecContext(ctx, path); err != nil {
			tx.Rollback()
			return fmt.Errorf("error inserting walked path (%s): %w", path, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// GetDeletedPaths returns up to limit paths under pathPrefix, sorted and strictly after `after`, whose latest version
// is not yet marked deleted but weren't seen in the current walk (i.e. newly-detected deletions).
func (d *Database) GetDeletedPaths(ctx context.Context, pathPrefix, after string, limit int) ([]string, error) {
	// Range scan instead of LIKE: exact, index-friendly, and no wildcard issues.
	rows, err := d.db.QueryContext(ctx, `
		SELECT f.path
		FROM files f
		WHERE f.path >= ?1 AND f.path < ?2 AND f.path > ?3
			AND f.deleted_at IS NULL
			AND f.version = (SELECT MAX(version) FROM files WHERE path = f.path)
			AND NOT EXISTS (SELECT 1 FROM walked_paths w WHERE w.path = f.path)
		ORDER BY f.path
		LIMIT ?4
	`, pathPrefix, pathPrefix+"\U0010FFFF", after, limit)
	if err != nil {
		return nil, fmt.Errorf("error querying deleted paths: %w", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("error scanning deleted path row: %w", err)
		}
		paths = append(paths, path)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over deleted path rows: %w", err)
	}

	return paths, nil
}
