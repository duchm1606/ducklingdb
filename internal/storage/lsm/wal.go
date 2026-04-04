package lsm

import (
	"errors"
	"io"
	"os"
)

// WAL is an append-only write-ahead log backed by a single file.
// Every write is encoded as an Entry and fsynced to disk before
// the caller considers it durable.
type WAL struct {
	path string
	fp   *os.File
}

// OpenWAL opens or creates a WAL file and fsyncs its parent directory.
func OpenWAL(path string) (*WAL, error) {
	fp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	if err := syncDir(path); err != nil {
		_ = fp.Close()
		return nil, err
	}

	return &WAL{
		path: path,
		fp:   fp,
	}, nil
}

// Append writes an encoded entry to the end of the WAL and fsyncs it.
func (w *WAL) Append(entry *Entry) error {
	if _, err := w.fp.Write(entry.Encode()); err != nil {
		return err
	}

	return w.fp.Sync()
}

// ReadAll replays all intact entries from the WAL.
// If the log ends with a partial or corrupted entry, replay stops there.
func (w *WAL) ReadAll() ([]Entry, error) {
	if _, err := w.fp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	entries := make([]Entry, 0)
	for {
		var entry Entry
		err := entry.Decode(w.fp)
		if err == nil {
			entries = append(entries, entry)
			continue
		}

		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrChecksumMismatch) {
			return entries, nil
		}

		return nil, err
	}
}

// WriteAll writes multiple encoded entries to the WAL in a single fsync.
// This amortizes the sync cost across a batch of writes.
func (w *WAL) WriteAll(entries []Entry) error {
	for i := range entries {
		if _, err := w.fp.Write(entries[i].Encode()); err != nil {
			return err
		}
	}
	return w.fp.Sync()
}

// Close closes the WAL file handle.
func (w *WAL) Close() error {
	return w.fp.Close()
}

// Remove closes the WAL and deletes the underlying file.
func (w *WAL) Remove() error {
	if err := w.Close(); err != nil {
		return err
	}

	return os.Remove(w.path)
}

// Truncate clears the WAL contents and resets the file position.
func (w *WAL) Truncate() error {
	if err := w.fp.Truncate(0); err != nil {
		return err
	}

	_, err := w.fp.Seek(0, io.SeekStart)
	return err
}
