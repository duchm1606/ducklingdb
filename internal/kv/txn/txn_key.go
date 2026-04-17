package txn

import (
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
)

// txnRecordPrefix prefixes all transaction record keys. The leading 0x00 byte
// places these records in a reserved system namespace. User keys, by
// convention, never start with 0x00 — this lets the engine contain both
// user data and transaction metadata without collision.
//
// Format: 0x00 "txn-" <16-byte TxnID>  →  total length 20 bytes.
var txnRecordPrefix = []byte{0x00, 't', 'x', 'n', '-'}

// TxnRecordKey returns the engine key at which the transaction record for id
// is stored. The same id always produces the same key, so any participant can
// locate the record from the TxnID alone.
func TxnRecordKey(id mvcc.TxnID) []byte {
	out := make([]byte, 0, len(txnRecordPrefix)+len(id))
	out = append(out, txnRecordPrefix...)
	out = append(out, id[:]...)
	return out
}
