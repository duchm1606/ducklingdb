package mvcc

import (
	"testing"
)

func TestMetadata_RoundTrip_NoIntent(t *testing.T) {
	t.Parallel()

	m := MVCCMetadata{
		Timestamp: ts(1_000_000, 0),
		Deleted:   false,
		KeyBytes:  8,
		ValBytes:  32,
	}
	data, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMetadata(data)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Timestamp.Equal(m.Timestamp) || got.Deleted != m.Deleted ||
		got.KeyBytes != m.KeyBytes || got.ValBytes != m.ValBytes {
		t.Fatalf("round trip: got %+v, want %+v", got, m)
	}
	if got.HasIntent() {
		t.Fatal("HasIntent must be false without Txn")
	}
}

func TestMetadata_RoundTrip_WithIntent(t *testing.T) {
	t.Parallel()

	txn := TxnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	m := MVCCMetadata{
		Timestamp:    ts(2_000_000, 0),
		Txn:          &txn,
		TxnTimestamp: ts(2_000_000, 5),
		KeyBytes:     4,
		ValBytes:     100,
	}
	data, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMetadata(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Txn == nil || *got.Txn != txn {
		t.Fatalf("txn round trip failed: got %v", got.Txn)
	}
	if !got.TxnTimestamp.Equal(m.TxnTimestamp) {
		t.Fatalf("TxnTimestamp lost: got %v, want %v", got.TxnTimestamp, m.TxnTimestamp)
	}
	if !got.HasIntent() {
		t.Fatal("HasIntent must be true with non-zero Txn")
	}
}

func TestMetadata_Deleted(t *testing.T) {
	t.Parallel()

	m := MVCCMetadata{Timestamp: ts(42, 0), Deleted: true}
	data, _ := m.Encode()
	got, _ := DecodeMetadata(data)
	if !got.Deleted {
		t.Fatal("Deleted flag lost")
	}
}

func TestTxnID_IsZero(t *testing.T) {
	t.Parallel()

	var zero TxnID
	if !zero.IsZero() {
		t.Fatal("zero value should be IsZero")
	}
	nonzero := TxnID{1}
	if nonzero.IsZero() {
		t.Fatal("non-zero should not be IsZero")
	}
}
