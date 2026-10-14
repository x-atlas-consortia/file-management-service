package core

import (
	"fmt"
	"os"
)

const DefaultProfile = "default"
const databasePath = "$HOME/.local/share/filescan/filescan-%s.db"

// GetDatabasePath returns the database path for the given profile.
func GetDatabasePath(profile string) string {
	if profile == "" {
		profile = DefaultProfile
	}

	dbPath := fmt.Sprintf(databasePath, profile)
	return os.ExpandEnv(dbPath)
}
