package lsm

const (
	sstableRecordHeaderSize = 9    // klen:4 + vlen:4 + tombstone:1
	sstableFooterSize       = 32   // record_count:8 + index_offset:8 + bloom_offset:8 + bloom_size:8
	defaultBloomFPRate      = 0.01 // 1% false positive rate
)
