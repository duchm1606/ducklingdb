//go:build unix

package lsm

import (
	"os"
	"path/filepath"
)

// syncDir fsyncs the parent directory of path to ensure
// the directory entry is durable on disk.
func syncDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()

	return dir.Sync()
}
