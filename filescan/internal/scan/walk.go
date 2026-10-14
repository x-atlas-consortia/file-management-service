package scan

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

type walkRoot struct {
	path  string
	files atomic.Int64
}

type walkItem struct {
	root *walkRoot
	dir  string
}

// walkFiles walks the provided directories with a fixed pool of walkers and sends file paths to filesCh.
// Pending directories sit in a shared stack, so memory stays proportional to the stack rather than to the
// number of directories. It returns the number of files sent.
func walkFiles(ctx context.Context, logger *slog.Logger, directories []string, ignorePatterns []string, filesCh chan<- string, walkers int) int64 {
	workerLogger := logger.With(slog.String("worker", "walk_files"))

	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	var stack []walkItem
	pending := 0 // directories queued or being read

	var roots []*walkRoot
	for _, dir := range directories {
		if _, err := os.Stat(dir); err != nil {
			workerLogger.Error("Error walking directory",
				slog.String("directory", dir),
				slog.String("error", fmt.Sprintf("cannot access directory %s: %v", dir, err)))
			continue
		}
		workerLogger.Info("Walking directory", slog.String("directory", dir))
		root := &walkRoot{path: dir}
		roots = append(roots, root)
		stack = append(stack, walkItem{root: root, dir: dir})
		pending++
	}

	next := func() (walkItem, bool) {
		mu.Lock()
		defer mu.Unlock()
		for len(stack) == 0 && pending > 0 {
			cond.Wait()
		}
		if len(stack) == 0 {
			return walkItem{}, false
		}
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		return item, true
	}

	finish := func(subdirs []walkItem) {
		mu.Lock()
		stack = append(stack, subdirs...)
		pending += len(subdirs) - 1
		mu.Unlock()
		cond.Broadcast()
	}

	var wg sync.WaitGroup
	for range max(walkers, 1) {
		wg.Go(func() {
			for {
				item, ok := next()
				if !ok {
					return
				}
				var subdirs []walkItem
				if ctx.Err() == nil {
					subdirs = readDirectory(ctx, workerLogger, item, ignorePatterns, filesCh)
				}
				finish(subdirs)
			}
		})
	}
	wg.Wait()

	var total int64
	for _, root := range roots {
		count := root.files.Load()
		total += count
		if err := ctx.Err(); err != nil {
			workerLogger.Error("Error walking directory",
				slog.String("directory", root.path),
				slog.String("error", err.Error()))
		} else {
			workerLogger.Info("Finished walking directory",
				slog.String("directory", root.path),
				slog.Int64("files", count))
		}
	}
	return total
}

// isIgnored reports whether any glob pattern matches the entry name or its full path.
// A trailing slash restricts the pattern to directories.
func isIgnored(patterns []string, name, path string, isDir bool) bool {
	for _, pattern := range patterns {
		if strings.HasSuffix(pattern, "/") {
			if !isDir {
				continue
			}
			pattern = strings.TrimRight(pattern, "/")
		}
		if ok, _ := filepath.Match(pattern, name); ok {
			return true
		}
		if ok, _ := filepath.Match(pattern, path); ok {
			return true
		}
	}
	return false
}

// readDirectory sends the regular files in item.dir to filesCh and returns its subdirectories.
func readDirectory(ctx context.Context, logger *slog.Logger, item walkItem, ignorePatterns []string, filesCh chan<- string) []walkItem {
	dir := item.dir

	f, err := os.Open(dir)
	if err != nil {
		logger.Warn("Error accessing path", slog.String("path", dir), slog.String("error", err.Error()))
		return nil
	}
	// Unsorted read; ReadDir may still return the entries read before an error
	entries, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		logger.Warn("Error accessing path", slog.String("path", dir), slog.String("error", err.Error()))
	}

	var subdirs []walkItem
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		mode := entry.Type()

		if isIgnored(ignorePatterns, entry.Name(), path, entry.IsDir()) {
			logger.Debug("Skipping ignored path", slog.String("path", path))
			continue
		}

		switch {
		case mode&os.ModeSymlink != 0:
			logger.Debug("Skipping symlink", slog.String("path", path))
		case mode.IsDir():
			subdirs = append(subdirs, walkItem{root: item.root, dir: path})
		case mode.IsRegular():
			select {
			case filesCh <- path:
				item.root.files.Add(1)
			case <-ctx.Done():
				return subdirs
			}
		}
	}
	return subdirs
}
