package txn

import (
	"bytes"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func TestTxnStatus_String(t *testing.T) {
	t.Parallel()

	cases := map[TxnStatus]string{
		TxnPending:    "PENDING",
		TxnCommitted:  "COMMITTED",
		TxnAborted:    "ABORTED",
		TxnStatus(99): "unknown(99)",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("%d: got %q want %q", int(s), got, want)
		}
	}
}

func TestIsolationLevel_String(t *testing.T) {
	t.Parallel()

	if SSI.String() != "SSI" {
		t.Fatalf("SSI: got %q", SSI.String())
	}
	if SI.String() != "SI" {
		t.Fatalf("SI: got %q", SI.String())
	}
}

func TestTransaction_IsFinalized(t *testing.T) {
	t.Parallel()

	pending := Transaction{Status: TxnPending}
	if pending.IsFinalized() {
		t.Fatal("PENDING transaction should not be finalized")
	}

	committed := Transaction{Status: TxnCommitted}
	if !committed.IsFinalized() {
		t.Fatal("COMMITTED transaction should be finalized")
	}

	aborted := Transaction{Status: TxnAborted}
	if !aborted.IsFinalized() {
		t.Fatal("ABORTED transaction should be finalized")
	}
}

func TestTransaction_AddIntentKey(t *testing.T) {
	t.Parallel()

	var txn Transaction
	txn.AddIntentKey([]byte("a"))
	txn.AddIntentKey([]byte("b"))
	txn.AddIntentKey([]byte("a")) // duplicate — should not be re-added

	if len(txn.IntentKeys) != 2 {
		t.Fatalf("expected 2 intent keys, got %d", len(txn.IntentKeys))
	}
	if !bytes.Equal(txn.IntentKeys[0], []byte("a")) {
		t.Fatalf("intent[0] = %q", txn.IntentKeys[0])
	}
	if !bytes.Equal(txn.IntentKeys[1], []byte("b")) {
		t.Fatalf("intent[1] = %q", txn.IntentKeys[1])
	}
}

func TestTransaction_AddIntentKey_CopiesSlice(t *testing.T) {
	t.Parallel()

	// The record must copy the caller's slice — if the caller mutates the
	// underlying array after AddIntentKey, the stored intent must be unaffected.
	key := []byte("alpha")
	var txn Transaction
	txn.AddIntentKey(key)

	key[0] = 'X' // caller mutation
	if !bytes.Equal(txn.IntentKeys[0], []byte("alpha")) {
		t.Fatalf("stored intent changed under caller mutation: %q", txn.IntentKeys[0])
	}
}

func TestTransaction_EncodeDecode_RoundTrip(t *testing.T) {
	t.Parallel()

	original := Transaction{
		ID:             mvcc.TxnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Status:         TxnPending,
		Isolation:      SSI,
		ReadTimestamp:  hlc.Timestamp{WallTime: 1000, Logical: 5},
		WriteTimestamp: hlc.Timestamp{WallTime: 1000, Logical: 5},
		MaxTimestamp:   hlc.Timestamp{WallTime: 1250, Logical: 0},
		Priority:       42,
		IntentKeys:     [][]byte{[]byte("k1"), []byte("k2")},
		LastHeartbeat:  hlc.Timestamp{WallTime: 1100, Logical: 0},
	}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeTransaction(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.ID != original.ID {
		t.Fatalf("ID mismatch: %v vs %v", decoded.ID, original.ID)
	}
	if decoded.Status != original.Status {
		t.Fatalf("Status: %v vs %v", decoded.Status, original.Status)
	}
	if decoded.Isolation != original.Isolation {
		t.Fatalf("Isolation: %v vs %v", decoded.Isolation, original.Isolation)
	}
	if decoded.ReadTimestamp != original.ReadTimestamp {
		t.Fatalf("ReadTimestamp: %v vs %v", decoded.ReadTimestamp, original.ReadTimestamp)
	}
	if decoded.WriteTimestamp != original.WriteTimestamp {
		t.Fatalf("WriteTimestamp: %v vs %v", decoded.WriteTimestamp, original.WriteTimestamp)
	}
	if decoded.MaxTimestamp != original.MaxTimestamp {
		t.Fatalf("MaxTimestamp: %v vs %v", decoded.MaxTimestamp, original.MaxTimestamp)
	}
	if decoded.Priority != original.Priority {
		t.Fatalf("Priority: %v vs %v", decoded.Priority, original.Priority)
	}
	if decoded.LastHeartbeat != original.LastHeartbeat {
		t.Fatalf("LastHeartbeat: %v vs %v", decoded.LastHeartbeat, original.LastHeartbeat)
	}
	if len(decoded.IntentKeys) != len(original.IntentKeys) {
		t.Fatalf("intent count: %d vs %d", len(decoded.IntentKeys), len(original.IntentKeys))
	}
	for i := range decoded.IntentKeys {
		if !bytes.Equal(decoded.IntentKeys[i], original.IntentKeys[i]) {
			t.Fatalf("intent[%d]: %q vs %q", i, decoded.IntentKeys[i], original.IntentKeys[i])
		}
	}
}

func TestTransaction_EncodeDecode_EmptyIntents(t *testing.T) {
	t.Parallel()

	original := Transaction{
		ID:        mvcc.TxnID{0xAA},
		Status:    TxnPending,
		Isolation: SSI,
		Priority:  1,
	}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeTransaction(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.IntentKeys) != 0 {
		t.Fatalf("expected empty intents, got %d", len(decoded.IntentKeys))
	}
}

func TestDecodeTransaction_Invalid(t *testing.T) {
	t.Parallel()

	if _, err := DecodeTransaction([]byte("not-json")); err == nil {
		t.Fatal("expected error decoding invalid JSON")
	}
}

func TestTxnRecordKey_Deterministic(t *testing.T) {
	t.Parallel()

	id := mvcc.TxnID{1, 2, 3, 4}
	k1 := TxnRecordKey(id)
	k2 := TxnRecordKey(id)
	if !bytes.Equal(k1, k2) {
		t.Fatalf("same ID produced different keys: %x vs %x", k1, k2)
	}
}

func TestTxnRecordKey_Unique(t *testing.T) {
	t.Parallel()

	id1 := mvcc.TxnID{1}
	id2 := mvcc.TxnID{2}
	if bytes.Equal(TxnRecordKey(id1), TxnRecordKey(id2)) {
		t.Fatal("different IDs produced the same key")
	}
}

func TestTxnRecordKey_ReservedPrefix(t *testing.T) {
	t.Parallel()

	// The leading 0x00 byte is what reserves this namespace. If the prefix ever
	// changes, this test will remind us to audit user-data conventions.
	key := TxnRecordKey(mvcc.TxnID{})
	if len(key) == 0 || key[0] != 0x00 {
		t.Fatalf("txn record key must start with 0x00, got %x", key)
	}
}
