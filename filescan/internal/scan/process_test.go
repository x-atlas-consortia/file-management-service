package scan

import (
	"path/filepath"
	"testing"
)

func TestDatasetUUIDFromPath(t *testing.T) {
	directoryUUID := "0123456789abcdef0123456789abcdef"
	filenameUUID := "abcdef0123456789abcdef0123456789"

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "extracts UUID from directory name",
			path: filepath.Join("archive", directoryUUID, "image.tif"),
			want: directoryUUID,
		},
		{
			name: "does not extract UUID from filename",
			path: filepath.Join("archive", filenameUUID+".tif"),
		},
		{
			name: "prefers directory UUID over filename UUID",
			path: filepath.Join("archive", directoryUUID, filenameUUID+".tif"),
			want: directoryUUID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := datasetUUIDFromPath(test.path); got != test.want {
				t.Errorf("datasetUUIDFromPath(%q) = %q, want %q", test.path, got, test.want)
			}
		})
	}
}
