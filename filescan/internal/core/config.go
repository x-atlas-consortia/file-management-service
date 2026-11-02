package core

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const configSection = "Default"

// Config holds the filescan settings read from an ini file.
type Config struct {
	Name              string // config file name without extension
	DatabaseFilepath  string
	LogDirectory      string
	Directories       []string
	PrunableFilepaths []string
	Walkers           int
	Processors        int
}

// LoadConfig reads and validates the ini file at path.
func LoadConfig(path string) (*Config, error) {
	values, err := parseINI(path, configSection)
	if err != nil {
		return nil, err
	}

	base := filepath.Base(path)
	cfg := &Config{
		Name:         strings.TrimSuffix(base, filepath.Ext(base)),
		LogDirectory: "./logs",
		Walkers:      4,
		Processors:   4,
	}

	// Datebase file path
	cfg.DatabaseFilepath = os.ExpandEnv(values["DATABASE_FILEPATH"])
	if cfg.DatabaseFilepath == "" {
		return nil, errors.New("DATABASE_FILEPATH is required in config")
	}

	// Log directory
	if v := os.ExpandEnv(values["LOG_DIRECTORY"]); v != "" {
		cfg.LogDirectory = v
	}

	// Directories to scan
	if cfg.Directories, err = parseList(values["DIRECTORIES"]); err != nil {
		return nil, fmt.Errorf("invalid DIRECTORIES: %w", err)
	}
	if len(cfg.Directories) < 1 {
		return nil, errors.New("DIRECTORIES must contain at least one directory")
	}

	// File patterns to skip
	if cfg.PrunableFilepaths, err = parseList(values["PRUNABLE_FILEPATHS"]); err != nil {
		return nil, fmt.Errorf("invalid PRUNABLE_FILEPATHS: %w", err)
	}
	for _, pattern := range cfg.PrunableFilepaths {
		if _, err := filepath.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("invalid prunable pattern %q: %w", pattern, err)
		}
	}

	// Number of walker goroutines
	if v, ok := values["WALKERS"]; ok {
		if cfg.Walkers, err = strconv.Atoi(v); err != nil || cfg.Walkers < 1 {
			return nil, errors.New("WALKERS must be an integer of at least 1")
		}
	}

	// Number of processor goroutines
	if v, ok := values["PROCESSORS"]; ok {
		if cfg.Processors, err = strconv.Atoi(v); err != nil || cfg.Processors < 1 {
			return nil, errors.New("PROCESSORS must be an integer of at least 1")
		}
	}

	return cfg, nil
}

// parseINI returns the key value pairs of the given section with uppercase keys.
func parseINI(path, section string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer f.Close()

	values := map[string]string{}
	inSection := false
	scanner := bufio.NewScanner(f)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		for strings.HasSuffix(line, "\\") && scanner.Scan() {
			lineNo++
			line = strings.TrimSpace(strings.TrimSuffix(line, "\\")) + " " + strings.TrimSpace(scanner.Text())
		}
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inSection = strings.EqualFold(strings.TrimSpace(line[1:len(line)-1]), section)
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("config line %d: expected key = value", lineNo)
		}
		if inSection {
			values[strings.ToUpper(strings.TrimSpace(key))] = strings.TrimSpace(value)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return values, nil
}

// parseList parses a bracketed list like ['a', "b"]. Quotes around items are optional.
func parseList(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return nil, fmt.Errorf("expected a list like ['a', 'b'], got %q", s)
	}
	var items []string
	for _, item := range strings.Split(s[1:len(s)-1], ",") {
		item = strings.TrimSpace(item)
		if len(item) >= 2 && (item[0] == '\'' || item[0] == '"') && item[len(item)-1] == item[0] {
			item = item[1 : len(item)-1]
		}
		if item != "" {
			items = append(items, item)
		}
	}
	return items, nil
}
