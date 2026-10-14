package scan

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"sync"

	"github.com/x-atlas-consortia/file-management-service/internal/database"
)

// processFiles reads paths from filesCh, reports each as walked, and sends changed or new files, with their
// checksums, to insertCh. It returns when filesCh is closed and drained or ctx is cancelled.
func processFiles(ctx context.Context, logger *slog.Logger, db *database.Database, filesCh <-chan string, insertCh chan<- database.InsertFileItem, walkedCh chan<- string) {
	for path := range filesCh {
		select {
		case walkedCh <- path:
		case <-ctx.Done():
			return
		}

		item, changed, err := buildFileItem(ctx, db, path)
		if err != nil {
			logger.Warn("Error processing file", slog.String("path", path), slog.String("error", err.Error()))
			continue
		}
		if !changed {
			continue
		}

		select {
		case insertCh <- item:
		case <-ctx.Done():
			return
		}
	}
}

// buildFileItem stats and hashes path unless the database already has an identical latest version.
func buildFileItem(ctx context.Context, db *database.Database, path string) (database.InsertFileItem, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return database.InsertFileItem{}, false, err
	}

	size := info.Size()
	modTime := info.ModTime().Unix()
	if modTime <= 0 {
		return database.InsertFileItem{}, false, fmt.Errorf("unsupported modification time %d", modTime)
	}

	state, latestVersion, err := db.GetFileState(ctx, path, size, modTime)
	if err != nil {
		return database.InsertFileItem{}, false, err
	}
	if state == database.FileUnchanged {
		return database.InsertFileItem{}, false, nil
	}

	datasetUUID := datasetUUIDRe.FindString(path)
	if datasetUUID == "" {
		return database.InsertFileItem{}, false, fmt.Errorf("no dataset UUID found in path %q", path)
	}

	sha256Sum, md5Sum, err := hashFile(path)
	if err != nil {
		return database.InsertFileItem{}, false, err
	}

	return database.InsertFileItem{
		IsNew:          state == database.FileNew,
		DatasetUUID:    datasetUUID,
		LastModifiedAt: modTime,
		MD5:            md5Sum,
		Path:           path,
		SHA256:         sha256Sum,
		Size:           size,
		Version:        latestVersion + 1,
	}, true, nil
}

// datasetUUIDRe matches the first 32-char lowercase hex run in a path.
var datasetUUIDRe = regexp.MustCompile(`[a-f0-9]{32}`)

// hashBufPool reuses copy buffers so each hashed file doesn't allocate its own.
var hashBufPool = sync.Pool{New: func() any { b := make([]byte, 256*1024); return &b }}

// hashFile computes the hex SHA-256 and MD5 of a file in a single read.
func hashFile(path string) (string, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	buf := hashBufPool.Get().(*[]byte)
	defer hashBufPool.Put(buf)

	sha := sha256.New()
	md := md5.New()
	if _, err := io.CopyBuffer(io.MultiWriter(sha, md), f, *buf); err != nil {
		return "", "", err
	}

	return hex.EncodeToString(sha.Sum(nil)), hex.EncodeToString(md.Sum(nil)), nil
}
