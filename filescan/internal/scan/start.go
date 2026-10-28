package scan

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/x-atlas-consortia/file-management-service/internal/core"
	"github.com/x-atlas-consortia/file-management-service/internal/database"
)

func Start(ctx context.Context, logLevel slog.Leveler, profile string, directories []string, ignorePatterns []string, walkers, processors int) error {
	startTime := time.Now().UTC()

	// Setup logger
	logger, logWriter, err := core.NewLogger("scan-start", profile, logLevel)
	if err != nil {
		slog.Error("Failed to create logger", "error", err)
		return err
	}

	// Setup context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dbPath := core.GetDatabasePath(profile)

	logger.Info("Starting file backup process",
		slog.Int("directories", len(directories)),
		slog.Int("walkers", walkers),
		slog.Int("processors", processors),
		slog.String("database", dbPath))

	// Setup channels and wait group for database insert worker
	fileInsertCh := make(chan database.InsertFileItem, 1000)
	walkedPathCh := make(chan string, 1000)
	dbReady := make(chan error, 1)
	dbResultCh := make(chan databaseWorkerResult, 1)
	var dbInsertWg sync.WaitGroup

	// Create database insert file worker
	dbInsertWg.Go(func() {
		result := databaseFileInsertWorker(ctx, logger, dbPath, directories, fileInsertCh, walkedPathCh, dbReady)
		dbResultCh <- result
		close(dbResultCh)
	})

	if err := <-dbReady; err != nil {
		logger.Error("Database initialization failed", slog.String("error", err.Error()))
		cancel()
		return err
	}
	close(dbReady)
	logger.Info("Database insert worker ready")

	// Create channels and wait group for process workers
	filesToProcessCh := make(chan string, 1000) // Buffer for file paths
	var processWg sync.WaitGroup

	// Read-only connection used by process workers to skip unchanged files
	lookupDB, err := database.New(ctx, dbPath, true)
	if err != nil {
		logger.Error("Error opening read-only database", slog.String("error", err.Error()))
		cancel()
		dbInsertWg.Wait()
		return err
	}
	defer func() {
		if closeErr := lookupDB.Close(ctx); closeErr != nil {
			logger.Error("Error closing read-only database", slog.String("error", closeErr.Error()))
		}
	}()

	for range processors {
		processWg.Go(func() {
			processFiles(ctx, logger, lookupDB, filesToProcessCh, fileInsertCh, walkedPathCh)
		})
	}

	// Walk files (blocks until complete)
	logger.Info("Starting file walking")
	filesScanned := walkFiles(ctx, logger, directories, ignorePatterns, filesToProcessCh, walkers)
	logger.Info("File walking completed, closing file channel")

	// Close the files channel to signal process workers no more files are coming
	close(filesToProcessCh)

	// Once processing is done, close the db worker's inputs so it flushes and finishes
	processWg.Wait()
	close(fileInsertCh)
	close(walkedPathCh)
	dbInsertWg.Wait()

	result := <-dbResultCh
	fmt.Fprintf(logWriter, "time=%s msg=\"File scan process completed\", files_scanned=%d files_inserted=%d files_modified=%d files_deleted=%d duration=%.2f seconds\n",
		time.Now().Format(time.RFC3339), filesScanned, result.FilesInserted, result.FilesModified, result.FilesDeleted, time.Since(startTime).Seconds())

	return ctx.Err()
}
