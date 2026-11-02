package cmd

import (
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/x-atlas-consortia/file-management-service/internal/core"
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

		cfg, ok := cmd.Context().Value("config").(*core.Config)
		if !ok {
			panic("config not found in context")
		}

		return scan.Start(cmd.Context(), logLevel, cfg)
	},
}

func init() {
	// Add scanStartCmd as a subcommand of scanCmd
	scanCmd.AddCommand(scanStartCmd)
	rootCmd.AddCommand(scanCmd)
}
