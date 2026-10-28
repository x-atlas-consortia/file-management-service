package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/x-atlas-consortia/file-management-service/internal/core"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "filescan",
	Short: "FileScan is a simple file scanning tool",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		profile, err := cmd.Flags().GetString("profile")
		if err != nil {
			return err
		}
		if profile == "" {
			profile = core.DefaultProfile
		}

		logLevelStr, err := cmd.Flags().GetString("log-level")
		if err != nil {
			return err
		}

		logLevel, err := core.ParseLogLevel(logLevelStr)
		if err != nil {
			return err
		}

		cmd.SetContext(context.WithValue(cmd.Context(), "log-level", logLevel))
		cmd.SetContext(context.WithValue(cmd.Context(), "profile", profile))

		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

// Initialize persistent flags
func init() {
	rootCmd.PersistentFlags().StringP("log-level", "l", "info", "Set the log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringP("profile", "p", "", "Set the config profile to use (overrides the default profile)")
}
