package proto

import (
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// ToHLC converts a proto Timestamp to an hlc.Timestamp.
func ToHLC(ts *Timestamp) hlc.Timestamp {
	if ts == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{WallTime: ts.WallTime, Logical: ts.Logical}
}

// FromHLC converts an hlc.Timestamp to a proto Timestamp.
func FromHLC(ts hlc.Timestamp) *Timestamp {
	return &Timestamp{WallTime: ts.WallTime, Logical: ts.Logical}
}

// ToTxnID converts a proto bytes field to an mvcc.TxnID.
func ToTxnID(b []byte) mvcc.TxnID {
	var id mvcc.TxnID
	copy(id[:], b)
	return id
}

// FromTxnID converts an mvcc.TxnID to bytes for proto serialization.
func FromTxnID(id mvcc.TxnID) []byte {
	b := make([]byte, len(id))
	copy(b, id[:])
	return b
}
