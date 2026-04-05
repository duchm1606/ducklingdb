package lsm

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

// SSTableWriter writes a sorted, immutable SSTable file to disk.
//
// File layout:
//
//	data records: [klen:4][vlen:4][tombstone:1][key][value]
//	offset index: repeated int64 offsets, one per record
//	bloom filter: [numBits:4][numHashes:4][bit_array...]
//	footer:       [record_count:8][index_offset:8][bloom_offset:8][bloom_size:8]
//
// The caller is responsible for providing keys in sorted order.
type SSTableWriter struct {
	fp      *os.File
	path    string
	offsets []int64
	keys    [][]byte // accumulated keys for bloom filter construction
	count   uint64
}

// NewSSTableWriter creates or truncates the SSTable file at path.
func NewSSTableWriter(path string) (*SSTableWriter, error) {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}

	return &SSTableWriter{
		fp:   fp,
		path: path,
	}, nil
}

// Add appends one sorted record to the SSTable data section.
func (w *SSTableWriter) Add(key, value []byte, tombstone bool) error {
	offset, err := w.fp.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	header := make([]byte, sstableRecordHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	if tombstone {
		header[8] = 1
	}

	if _, err := w.fp.Write(header); err != nil {
		return err
	}
	if _, err := w.fp.Write(key); err != nil {
		return err
	}
	if _, err := w.fp.Write(value); err != nil {
		return err
	}

	w.offsets = append(w.offsets, offset)
	w.keys = append(w.keys, append([]byte{}, key...))
	w.count++
	return nil
}

// Finish writes the offset index, bloom filter, and footer, then fsyncs and
// closes the file.
func (w *SSTableWriter) Finish() error {
	// Write offset index.
	indexOffset, err := w.fp.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	for _, offset := range w.offsets {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(offset))
		if _, err := w.fp.Write(buf); err != nil {
			return err
		}
	}

	// Build and write bloom filter.
	bloomOffset, err := w.fp.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	bf := NewBloomFilter(max(int(w.count), 1), defaultBloomFPRate)
	for _, key := range w.keys {
		bf.Add(key)
	}
	bloomData := bf.Encode()
	if _, err := w.fp.Write(bloomData); err != nil {
		return err
	}

	// Write footer.
	footer := make([]byte, sstableFooterSize)
	binary.LittleEndian.PutUint64(footer[0:8], w.count)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(len(bloomData)))
	if _, err := w.fp.Write(footer); err != nil {
		return err
	}

	if err := w.fp.Sync(); err != nil {
		return err
	}

	return w.Close()
}

// WriteSSTableFromIterator turns any ordered iterator into one SSTable file
// by streaming keys in iterator order. The iterator is expected to already
// be sorted.
func WriteSSTableFromIterator(path string, iter storage.Iterator) error {
	writer, err := NewSSTableWriter(path)
	if err != nil {
		return err
	}

	defer iter.Close()

	if iter.Seek([]byte("")) {
		for iter.Valid() {
			if err := writer.Add(iter.Key(), iter.Value(), iter.IsTombstone()); err != nil {
				_ = writer.Close()
				return err
			}
			iter.Next()
		}
	}

	return writer.Finish()
}

// Close releases the underlying file handle.
func (w *SSTableWriter) Close() error {
	return w.fp.Close()
}
