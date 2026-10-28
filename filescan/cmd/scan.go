package cmd

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/x-atlas-consortia/file-management-service/internal/scan"
)

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "Scan operations",
}

var scanStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a new file scan",
	RunE: func(cmd *cobra.Command, args []string) error {
		logLevel, ok := cmd.Context().Value("log-level").(slog.Leveler)
		if !ok {
			panic("log-level not found in context")
		}

		profile, ok := cmd.Context().Value("profile").(string)
		if !ok {
			panic("profile not found in context")
		}

		directories, err := cmd.Flags().GetStringSlice("directories")
		if err != nil {
			return err
		}
		if len(directories) < 1 {
			return errors.New("at least one directory must be specified")
		}

		walkers, err := cmd.Flags().GetInt("walkers")
		if err != nil {
			return err
		}
		if walkers < 1 {
			return errors.New("walkers must be at least 1")
		}

		processors, err := cmd.Flags().GetInt("processors")
		if err != nil {
			return err
		}
		if processors < 1 {
			return errors.New("processors must be at least 1")
		}

		ignorePatterns, err := cmd.Flags().GetStringSlice("ignore")
		if err != nil {
			return err
		}
		for _, pattern := range ignorePatterns {
			if _, err := filepath.Match(pattern, ""); err != nil {
				return fmt.Errorf("invalid ignore pattern %q: %w", pattern, err)
			}
		}

		return scan.Start(cmd.Context(), logLevel, profile, directories, ignorePatterns, walkers, processors)
	},
}

func init() {
	// Add scanStartCmd as a subcommand of scanCmd
	scanCmd.AddCommand(scanStartCmd)
	rootCmd.AddCommand(scanCmd)

	// Scan start flags
	// directories flag
	scanStartCmd.Flags().StringSliceP("directories", "D", []string{}, "List of directories to scan (multiple -D flags allowed or comma-separated string)")
	scanStartCmd.MarkFlagRequired("directories")

	// ignore flag
	scanStartCmd.Flags().StringSliceP("ignore", "i", []string{}, "Glob patterns to ignore, matched against file/directory names and full paths (multiple -i flags allowed or comma-separated)")

	// walkers flag
	scanStartCmd.Flags().Int("walkers", 4, "Number of concurrent directory walkers (default: 4)")

	// processors flag
	scanStartCmd.Flags().Int("processors", 4, "Number of concurrent file processors (default: 4)")
}
