package mvcc

import (
	"bytes"
	"sort"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func ts(wall int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{WallTime: wall, Logical: logical}
}

func TestEncode_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := []MVCCKey{
		{Key: []byte("user:42"), Timestamp: ts(1, 0)},
		{Key: []byte("user:42"), Timestamp: ts(1_000_000_000, 99)},
		{Key: []byte(""), Timestamp: ts(10, 0)},
		{Key: []byte{0x00, 0x01, 0xff}, Timestamp: ts(500, 5)}, // raw key contains 0x00
	}

	for _, c := range cases {
		encoded := Encode(c)
		got, versioned, err := Decode(encoded)
		if err != nil {
			t.Fatalf("decode(%q) error: %v", c.Key, err)
		}
		if !versioned {
			t.Fatalf("expected versioned, got metadata for %v", c)
		}
		if !bytes.Equal(got.Key, c.Key) {
			t.Fatalf("key mismatch: got %q, want %q", got.Key, c.Key)
		}
		if !got.Timestamp.Equal(c.Timestamp) {
			t.Fatalf("timestamp mismatch: got %v, want %v", got.Timestamp, c.Timestamp)
		}

		// Encode should be deterministic.
		if !bytes.Equal(encoded, Encode(got)) {
			t.Fatalf("encode not deterministic")
		}
	}
}

func TestEncodeMeta_RoundTrip(t *testing.T) {
	t.Parallel()

	encoded := EncodeMeta([]byte("user:42"))
	got, versioned, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if versioned {
		t.Fatal("expected metadata, got versioned")
	}
	if !bytes.Equal(got.Key, []byte("user:42")) {
		t.Fatalf("key mismatch: %q", got.Key)
	}
	if !got.Timestamp.IsEmpty() {
		t.Fatalf("expected empty timestamp, got %v", got.Timestamp)
	}
}

func TestEncode_InlineIsMetadata(t *testing.T) {
	t.Parallel()

	// Inline key (zero timestamp) must encode identically to EncodeMeta.
	k := MVCCKey{Key: []byte("hello")}
	if !bytes.Equal(Encode(k), EncodeMeta(k.Key)) {
		t.Fatal("inline key should equal metadata encoding")
	}
}

func TestOrdering_NewestFirst(t *testing.T) {
	t.Parallel()

	// Same raw key at 10 different timestamps. After sorting the encoded
	// bytes, the metadata must come first, then newest → oldest.
	raw := []byte("k")

	encoded := [][]byte{EncodeMeta(raw)}
	for i := int64(1); i <= 10; i++ {
		encoded = append(encoded, Encode(MVCCKey{Key: raw, Timestamp: ts(i, 0)}))
	}
	sort.Slice(encoded, func(i, j int) bool { return bytes.Compare(encoded[i], encoded[j]) < 0 })

	// First must be metadata.
	_, versioned, _ := Decode(encoded[0])
	if versioned {
		t.Fatal("metadata should sort first")
	}

	// Remaining must decode with descending timestamps.
	var prevWall int64 = 1<<63 - 1
	for i := 1; i < len(encoded); i++ {
		k, ok, err := Decode(encoded[i])
		if err != nil || !ok {
			t.Fatalf("index %d: decode failed: %v", i, err)
		}
		if k.Timestamp.WallTime >= prevWall {
			t.Fatalf("index %d: %v not strictly less than previous %d", i, k.Timestamp, prevWall)
		}
		prevWall = k.Timestamp.WallTime
	}
}

func TestOrdering_KeysDoNotInterleave(t *testing.T) {
	t.Parallel()

	// All versions of "aaa" must sort before any version of "bbb".
	encoded := [][]byte{
		Encode(MVCCKey{Key: []byte("aaa"), Timestamp: ts(1, 0)}),
		Encode(MVCCKey{Key: []byte("aaa"), Timestamp: ts(100, 0)}),
		Encode(MVCCKey{Key: []byte("bbb"), Timestamp: ts(1, 0)}),
		Encode(MVCCKey{Key: []byte("bbb"), Timestamp: ts(100, 0)}),
	}
	sort.Slice(encoded, func(i, j int) bool { return bytes.Compare(encoded[i], encoded[j]) < 0 })

	for i := 0; i < 2; i++ {
		k, _, _ := Decode(encoded[i])
		if !bytes.Equal(k.Key, []byte("aaa")) {
			t.Fatalf("index %d: expected aaa, got %q", i, k.Key)
		}
	}
	for i := 2; i < 4; i++ {
		k, _, _ := Decode(encoded[i])
		if !bytes.Equal(k.Key, []byte("bbb")) {
			t.Fatalf("index %d: expected bbb, got %q", i, k.Key)
		}
	}
}

func TestDecode_Errors(t *testing.T) {
	t.Parallel()

	if _, _, err := Decode(nil); err == nil {
		t.Fatal("expected error on empty input")
	}
	if _, _, err := Decode([]byte("nosep")); err == nil {
		t.Fatal("expected error on missing separator")
	}
}

func TestOrdering_LargeKeyspace(t *testing.T) {
	t.Parallel()

	// Stress check: metadata for every key comes first, then versioned entries
	// grouped by raw key and descending by timestamp.
	keys := []string{"a", "ab", "b", "ba"}
	var encoded [][]byte
	for _, k := range keys {
		encoded = append(encoded, EncodeMeta([]byte(k)))
		for _, wall := range []int64{1, 5, 10, 100} {
			encoded = append(encoded, Encode(MVCCKey{Key: []byte(k), Timestamp: ts(wall, 0)}))
		}
	}
	sort.Slice(encoded, func(i, j int) bool { return bytes.Compare(encoded[i], encoded[j]) < 0 })

	// Decode all and verify group-and-descending invariant.
	var lastKey []byte
	var lastWall int64
	var sawMeta bool
	for _, e := range encoded {
		k, ver, err := Decode(e)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}

		if lastKey == nil || !bytes.Equal(lastKey, k.Key) {
			if ver {
				t.Fatalf("new raw key %q must start with metadata", k.Key)
			}
			lastKey = k.Key
			lastWall = 0
			sawMeta = true
			continue
		}

		// Same raw key as previous.
		if !sawMeta {
			t.Fatalf("metadata missing for %q", k.Key)
		}
		if !ver {
			t.Fatalf("unexpected second metadata for %q", k.Key)
		}
		if lastWall != 0 && k.Timestamp.WallTime >= lastWall {
			t.Fatalf("timestamps not descending: %d >= %d for key %q", k.Timestamp.WallTime, lastWall, k.Key)
		}
		lastWall = k.Timestamp.WallTime
	}
}
