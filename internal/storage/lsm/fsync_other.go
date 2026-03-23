//go:build !unix

package lsm

// syncDir is a no-op on non-Unix platforms.
func syncDir(path string) error {
	return nil
}
