package executor

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/sql/catalog"
)

// encodeRowKey builds the KV key for a row: /<tablename>/<encoded_pk>
func encodeRowKey(tableName string, pkType catalog.ColumnType, pkVal any) []byte {
	prefix := "/" + tableName + "/"
	switch pkType {
	case catalog.TypeINT:
		buf := make([]byte, len(prefix)+8)
		copy(buf, prefix)
		binary.BigEndian.PutUint64(buf[len(prefix):], uint64(toInt64(pkVal)))
		return buf
	default: // TEXT
		return append([]byte(prefix), fmt.Sprintf("%v", pkVal)...)
	}
}

// tableKeyRange returns [start, end) covering all rows in tableName.
func tableKeyRange(tableName string) (start, end []byte) {
	prefix := "/" + tableName + "/"
	start = []byte(prefix)
	end = append([]byte(prefix), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
	return
}

// encodeRowValue serializes non-PK columns to JSON.
func encodeRowValue(cols []catalog.Column, row map[string]any) ([]byte, error) {
	out := make(map[string]any, len(row))
	for _, col := range cols {
		if col.PrimaryKey {
			continue
		}
		if v, ok := row[col.Name]; ok {
			out[col.Name] = v
		}
	}
	return json.Marshal(out)
}

// decodeRowValue deserializes a JSON row value back to a map.
func decodeRowValue(data []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// toInt64 coerces numeric types to int64.
func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

// decodePKFromKey extracts the primary key value as a string from a row key.
func decodePKFromKey(key []byte, tableName string, pkType catalog.ColumnType) string {
	prefix := "/" + tableName + "/"
	if len(key) <= len(prefix) {
		return ""
	}
	raw := key[len(prefix):]
	switch pkType {
	case catalog.TypeINT:
		if len(raw) < 8 {
			return ""
		}
		n := int64(binary.BigEndian.Uint64(raw))
		return fmt.Sprintf("%d", n)
	default:
		return string(raw)
	}
}
