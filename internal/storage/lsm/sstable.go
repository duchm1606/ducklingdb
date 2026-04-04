package lsm

const (
	sstableRecordHeaderSize = 9  // klen:4 + vlen:4 + tombstone:1
	sstableFooterSize       = 16 // record_count:8 + index_offset:8
)
