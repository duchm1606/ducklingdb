package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestEntryRoundTrip(t *testing.T) {
	t.Parallel()

	original := Entry{
		Key:   []byte("name"),
		Value: []byte("alice"),
		Op:    OpPut,
	}

	var decoded Entry
	err := decoded.Decode(bytes.NewReader(original.Encode()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	assertEntryEqual(t, original, decoded)
}

func TestEntryRoundTripDelete(t *testing.T) {
	t.Parallel()

	original := Entry{
		Key: []byte("name"),
		Op:  OpDelete,
	}

	var decoded Entry
	err := decoded.Decode(bytes.NewReader(original.Encode()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	assertEntryEqual(t, original, decoded)
}

func TestEntryRoundTripEmptyKey(t *testing.T) {
	t.Parallel()

	original := Entry{
		Key:   []byte{},
		Value: []byte("value"),
		Op:    OpPut,
	}

	var decoded Entry
	err := decoded.Decode(bytes.NewReader(original.Encode()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	assertEntryEqual(t, original, decoded)
}

func TestEntryRoundTripLargePayload(t *testing.T) {
	t.Parallel()

	original := Entry{
		Key:   bytes.Repeat([]byte("k"), 1<<20),
		Value: bytes.Repeat([]byte("v"), 1<<20),
		Op:    OpPut,
	}

	var decoded Entry
	err := decoded.Decode(bytes.NewReader(original.Encode()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	assertEntryEqual(t, original, decoded)
}

func TestEntryDecodeCorruptedChecksum(t *testing.T) {
	t.Parallel()

	encoded := Entry{
		Key:   []byte("name"),
		Value: []byte("alice"),
		Op:    OpPut,
	}.Encode()
	encoded[len(encoded)-1] ^= 0x01

	var decoded Entry
	err := decoded.Decode(bytes.NewReader(encoded))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Decode() error = %v, want %v", err, ErrChecksumMismatch)
	}
}

func TestEntryDecodePartialHeader(t *testing.T) {
	t.Parallel()

	var decoded Entry
	err := decoded.Decode(bytes.NewReader([]byte{1, 2, 3, 4, 5}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Decode() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestEntryDecodeEOF(t *testing.T) {
	t.Parallel()

	var decoded Entry
	err := decoded.Decode(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Decode() error = %v, want %v", err, io.EOF)
	}
}

func TestMultipleEntriesSequential(t *testing.T) {
	t.Parallel()

	entries := make([]Entry, 100)
	var buffer bytes.Buffer

	for index := range entries {
		entries[index] = Entry{
			Key:   []byte(fmt.Sprintf("key-%03d", index)),
			Value: []byte(fmt.Sprintf("value-%03d", index)),
			Op:    OpPut,
		}
		if _, err := buffer.Write(entries[index].Encode()); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	reader := bytes.NewReader(buffer.Bytes())
	for _, original := range entries {
		var decoded Entry
		if err := decoded.Decode(reader); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		assertEntryEqual(t, original, decoded)
	}

	var final Entry
	err := final.Decode(reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("final Decode() error = %v, want %v", err, io.EOF)
	}
}

func assertEntryEqual(t *testing.T, want, got Entry) {
	t.Helper()

	if want.Op != got.Op {
		t.Fatalf("Op = %d, want %d", got.Op, want.Op)
	}

	if !bytes.Equal(want.Key, got.Key) {
		t.Fatalf("Key = %q, want %q", got.Key, want.Key)
	}

	if !bytes.Equal(want.Value, got.Value) {
		t.Fatalf("Value = %q, want %q", got.Value, want.Value)
	}
}
