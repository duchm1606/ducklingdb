package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// SSTableReader opens an immutable SSTable file for point lookups and iteration.
type SSTableReader struct {
	fp      *os.File
	path    string
	offsets []int64
	bloom   *BloomFilter
	count   int
}

type sstableRecord struct {
	key       []byte
	value     []byte
	tombstone bool
}

// OpenSSTable opens an SSTable, reads the footer, loads the offset index,
// and loads the bloom filter.
func OpenSSTable(path string) (*SSTableReader, error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := fp.Stat()
	if err != nil {
		_ = fp.Close()
		return nil, err
	}
	if info.Size() < sstableFooterSize {
		_ = fp.Close()
		return nil, fmt.Errorf("sstable too small: %d bytes", info.Size())
	}

	// Read footer.
	footer := make([]byte, sstableFooterSize)
	footerOffset := info.Size() - sstableFooterSize
	if _, err := fp.ReadAt(footer, footerOffset); err != nil {
		_ = fp.Close()
		return nil, err
	}

	recordCount := binary.LittleEndian.Uint64(footer[0:8])
	indexOffset := binary.LittleEndian.Uint64(footer[8:16])
	bloomOffset := binary.LittleEndian.Uint64(footer[16:24])
	bloomSize := binary.LittleEndian.Uint64(footer[24:32])

	// Load offset index.
	offsets := make([]int64, recordCount)
	if recordCount > 0 {
		if _, err := fp.Seek(int64(indexOffset), io.SeekStart); err != nil {
			_ = fp.Close()
			return nil, err
		}

		buf := make([]byte, 8)
		for i := 0; i < int(recordCount); i++ {
			if _, err := io.ReadFull(fp, buf); err != nil {
				_ = fp.Close()
				return nil, fmt.Errorf("read offset index: %w", err)
			}
			offsets[i] = int64(binary.LittleEndian.Uint64(buf))
		}
	}

	// Load bloom filter.
	var bloom *BloomFilter
	if bloomSize > 0 {
		bloomData := make([]byte, bloomSize)
		if _, err := fp.ReadAt(bloomData, int64(bloomOffset)); err != nil {
			_ = fp.Close()
			return nil, fmt.Errorf("read bloom filter: %w", err)
		}
		bloom = DecodeBloomFilter(bloomData)
	}

	return &SSTableReader{
		fp:      fp,
		path:    path,
		offsets: offsets,
		bloom:   bloom,
		count:   int(recordCount),
	}, nil
}

func (r *SSTableReader) readRecordAt(offset int64) (*sstableRecord, error) {
	if _, err := r.fp.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	header := make([]byte, sstableRecordHeaderSize)
	if _, err := io.ReadFull(r.fp, header); err != nil {
		return nil, err
	}

	klen := binary.LittleEndian.Uint32(header[0:4])
	vlen := binary.LittleEndian.Uint32(header[4:8])
	tombstone := header[8] == 1

	key := make([]byte, klen)
	if _, err := io.ReadFull(r.fp, key); err != nil {
		return nil, err
	}

	value := make([]byte, vlen)
	if _, err := io.ReadFull(r.fp, value); err != nil {
		return nil, err
	}

	return &sstableRecord{
		key:       key,
		value:     value,
		tombstone: tombstone,
	}, nil
}

// Get performs a binary search over the record offsets and returns the value for key.
// Returns (value, found, tombstone, error). When found is true and tombstone is true,
// the key exists as a deletion marker and must shadow older values.
//
// If the SSTable has a bloom filter, it is checked first. A "definitely not"
// result skips the binary search entirely.
func (r *SSTableReader) Get(key []byte) ([]byte, bool, bool, error) {
	if r.bloom != nil && !r.bloom.MayContain(key) {
		return nil, false, false, nil
	}

	left := 0
	right := len(r.offsets) - 1

	for left <= right {
		mid := left + (right-left)/2
		record, err := r.readRecordAt(r.offsets[mid])
		if err != nil {
			return nil, false, false, err
		}

		cmp := bytes.Compare(record.key, key)
		if cmp == 0 {
			if record.tombstone {
				return nil, true, true, nil
			}
			return record.value, true, false, nil
		}

		if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil, false, false, nil
}

// SSTableIterator:
// The SSTable does not keep all keys in memory. Instead, the file itself is the
// sorted structure, and `offsets[i]` tells us where the i-th sorted record lives
// on disk. That is enough for two important operations:
//  1. Binary search: probe offsets[mid], read that record's key, compare, then
//     move left or right.
//  2. Iteration: keep a current index into `offsets`, read the record at that
//     position, and move forward/backward by changing the index.
//
// We need this iterator because SSTables are not only used for point lookups.
// They are also inputs to range scans, merge iteration, and later compaction.
// The iterator lets the SSTable participate in the same ordered traversal model
// as the MemTable, without loading the entire table into memory.

type SSTableIterator struct {
	reader *SSTableReader
	index  int
	record *sstableRecord
}

func (r *SSTableReader) NewIterator() *SSTableIterator {
	return &SSTableIterator{reader: r, index: -1}
}

func (it *SSTableIterator) loadRecord() bool {
	if it.index < 0 || it.index >= len(it.reader.offsets) {
		it.record = nil
		return false
	}

	record, err := it.reader.readRecordAt(it.reader.offsets[it.index])
	if err != nil {
		it.record = nil
		return false
	}

	it.record = record
	return true
}

func (it *SSTableIterator) Seek(key []byte) bool {
	left := 0
	right := len(it.reader.offsets)

	for left < right {
		mid := left + (right-left)/2
		record, err := it.reader.readRecordAt(it.reader.offsets[mid])
		if err != nil {
			it.index = -1
			it.record = nil
			return false
		}

		if bytes.Compare(record.key, key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	it.index = left
	return it.loadRecord()
}

func (it *SSTableIterator) Next() bool {
	if it.index < 0 {
		it.index = 0
	} else {
		it.index++
	}

	return it.loadRecord()
}

func (it *SSTableIterator) Prev() bool {
	if it.index < 0 {
		it.index = len(it.reader.offsets) - 1
	} else {
		it.index--
	}

	return it.loadRecord()
}

func (it *SSTableIterator) Valid() bool {
	return it.record != nil
}

func (it *SSTableIterator) Key() []byte {
	return it.record.key
}

func (it *SSTableIterator) Value() []byte {
	return it.record.value
}

func (it *SSTableIterator) IsTombstone() bool {
	return it.record.tombstone
}

func (it *SSTableIterator) Close() {}

// Close releases the underlying file handle.
func (r *SSTableReader) Close() error {
	return r.fp.Close()
}
