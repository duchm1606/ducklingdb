package lsm

import (
	"fmt"
	"testing"
)

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	t.Parallel()

	bf := NewBloomFilter(1000, 0.01)
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
		bf.Add(keys[i])
	}

	for _, key := range keys {
		if !bf.MayContain(key) {
			t.Fatalf("MayContain(%q) = false, want true (false negative)", key)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	t.Parallel()

	n := 10000
	bf := NewBloomFilter(n, 0.01)
	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("key-%06d", i)))
	}

	// Test keys that were NOT added.
	falsePositives := 0
	tests := 10000
	for i := 0; i < tests; i++ {
		key := []byte(fmt.Sprintf("miss-%06d", i))
		if bf.MayContain(key) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / float64(tests)
	// Allow up to 2% — the target is 1%, but randomness needs headroom.
	if rate > 0.02 {
		t.Fatalf("false positive rate = %.4f (%d/%d), want < 0.02", rate, falsePositives, tests)
	}
	t.Logf("false positive rate = %.4f (%d/%d)", rate, falsePositives, tests)
}

func TestBloomFilter_DefinitelyNot(t *testing.T) {
	t.Parallel()

	bf := NewBloomFilter(100, 0.01)
	bf.Add([]byte("alice"))
	bf.Add([]byte("bob"))

	// With only 2 keys in a 100-key filter, most misses should return false.
	misses := 0
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("stranger-%03d", i))
		if !bf.MayContain(key) {
			misses++
		}
	}

	if misses < 90 {
		t.Fatalf("only %d/100 definite misses, expected most to be filtered", misses)
	}
}

func TestBloomFilter_EncodeDecode(t *testing.T) {
	t.Parallel()

	bf := NewBloomFilter(1000, 0.01)
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
		bf.Add(keys[i])
	}

	encoded := bf.Encode()
	decoded := DecodeBloomFilter(encoded)

	// All keys that were in the original must be in the decoded filter.
	for _, key := range keys {
		if !decoded.MayContain(key) {
			t.Fatalf("decoded MayContain(%q) = false after round-trip", key)
		}
	}

	// Spot-check a missing key.
	if decoded.MayContain([]byte("definitely-not-here-xyz")) {
		// This could be a false positive, but is very unlikely for a single key.
		// Don't fail, just note it.
		t.Log("unlikely false positive on spot-check key")
	}
}

func TestBloomFilter_Empty(t *testing.T) {
	t.Parallel()

	bf := NewBloomFilter(100, 0.01)

	// Nothing was added, so everything should be "definitely not".
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if bf.MayContain(key) {
			t.Fatalf("MayContain(%q) = true on empty filter", key)
		}
	}
}

func TestBloomFilter_SmallFilter(t *testing.T) {
	t.Parallel()

	bf := NewBloomFilter(1, 0.01)
	bf.Add([]byte("only"))

	if !bf.MayContain([]byte("only")) {
		t.Fatal("MayContain(only) = false, want true")
	}
}
