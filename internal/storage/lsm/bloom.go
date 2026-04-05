package lsm

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

const (
	ln2       = 0.693147180559945
	ln2Square = 0.480453013918201

	// bloomEncodeSize returns the number of bytes needed to serialize this filter.
	// Format: [numBits:4][numHashes:4][bit_array...]
	bloomHeaderSize = 8
)

// BloomFilter is a space-efficient probabilistic data structure that tests
// whether a key is a member of a set. False positives are possible, but
// false negatives are not: if MayContain returns false, the key is
// definitely not in the set.
type BloomFilter struct {
	bits      []byte
	numBits   uint32
	numHashes uint32
}

// NewBloomFilter creates a bloom filter sized for the expected number of keys
// and desired false positive rate. For example, NewBloomFilter(1000, 0.01)
// creates a filter for 1000 keys with a 1% false positive rate.
func NewBloomFilter(numKeys int, falsePositiveRate float64) *BloomFilter {
	if numKeys < 1 {
		numKeys = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	// Optimal bits per key: -ln(errorRate) / ln(2)^2
	bitsPerKey := math.Abs(math.Log(falsePositiveRate) / ln2Square)

	// Total bits, rounded up to the next multiple of 8
	numBits := uint32(math.Ceil(float64(numKeys) * bitsPerKey))
	if numBits%8 != 0 {
		numBits = ((numBits / 8) + 1) * 8
	}
	if numBits < 8 {
		numBits = 8
	}

	// Optimal number of hash functions: bitsPerKey * ln(2)
	numHashes := uint32(math.Ceil(bitsPerKey * ln2))
	if numHashes < 1 {
		numHashes = 1
	}

	return &BloomFilter{
		bits:      make([]byte, numBits/8),
		numBits:   numBits,
		numHashes: numHashes,
	}
}

// bloomHash computes two independent 64-bit hashes from a key using FNV-64a
// with two different single-byte prefixes. The prefix changes the hash's
// internal state before processing the key, producing independent outputs.
// These two values are combined to simulate k independent hash functions via:
// hash_i = (a + b*i) % numBits.
func bloomHash(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write([]byte{0x01}) // prefix diverges internal FNV state
	h1.Write(key)

	h2 := fnv.New64a()
	h2.Write([]byte{0x02}) // different prefix -> independent output
	h2.Write(key)

	return h1.Sum64(), h2.Sum64()
}

// Add inserts a key into the bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	a, b := bloomHash(key)
	m := uint64(bf.numBits)
	for i := uint32(0); i < bf.numHashes; i++ {
		pos := (a + b*uint64(i)) % m
		// Bit to byte mapping:
		// pos / 8 = byte index, pos % 8 = bit within that byte.
		// use bitwise OR to set the bit for better performance
		bf.bits[pos>>3] |= 1 << (pos % 8)
	}
}

// MayContain returns true if the key might be in the set, false if it is
// definitely not. A true result may be a false positive; a false result
// is always correct.
func (bf *BloomFilter) MayContain(key []byte) bool {
	a, b := bloomHash(key)
	m := uint64(bf.numBits)
	for i := uint32(0); i < bf.numHashes; i++ {
		pos := (a + b*uint64(i)) % m
		if bf.bits[pos>>3]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the bloom filter into a byte slice.
func (bf *BloomFilter) Encode() []byte {
	buf := make([]byte, bloomHeaderSize+len(bf.bits))
	binary.LittleEndian.PutUint32(buf[0:4], bf.numBits)
	binary.LittleEndian.PutUint32(buf[4:8], bf.numHashes)
	copy(buf[bloomHeaderSize:], bf.bits)
	return buf
}

// DecodeBloomFilter deserializes a bloom filter from a byte slice previously
// produced by Encode.
func DecodeBloomFilter(data []byte) *BloomFilter {
	numBits := binary.LittleEndian.Uint32(data[0:4])
	numHashes := binary.LittleEndian.Uint32(data[4:8])
	bits := make([]byte, len(data)-bloomHeaderSize)
	copy(bits, data[bloomHeaderSize:])
	return &BloomFilter{
		bits:      bits,
		numBits:   numBits,
		numHashes: numHashes,
	}
}
