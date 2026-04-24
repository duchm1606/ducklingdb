package executor

import (
	"bytes"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/sql/catalog"
)

func TestEncodeIntKey(t *testing.T) {
	key := encodeRowKey("users", catalog.TypeINT, int64(42))
	if len(key) == 0 {
		t.Fatal("empty key")
	}
	k1 := encodeRowKey("users", catalog.TypeINT, int64(1))
	k2 := encodeRowKey("users", catalog.TypeINT, int64(2))
	if bytes.Compare(k1, k2) >= 0 {
		t.Fatal("int keys must sort in ascending order")
	}
}

func TestEncodeTextKey(t *testing.T) {
	key := encodeRowKey("users", catalog.TypeTEXT, "alice")
	if len(key) == 0 {
		t.Fatal("empty key")
	}
}

func TestTableKeyRange(t *testing.T) {
	start, end := tableKeyRange("users")
	key := encodeRowKey("users", catalog.TypeINT, int64(1))
	if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
		t.Fatal("key should be within table range")
	}
}

func TestEncodeDecodeValue(t *testing.T) {
	cols := []catalog.Column{
		{Name: "id",   Type: catalog.TypeINT,  PrimaryKey: true},
		{Name: "name", Type: catalog.TypeTEXT, PrimaryKey: false},
		{Name: "age",  Type: catalog.TypeINT,  PrimaryKey: false},
	}
	row := map[string]any{
		"name": "alice",
		"age":  int64(30),
	}
	data, err := encodeRowValue(cols, row)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeRowValue(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["name"] != "alice" {
		t.Fatalf("name: got %v", got["name"])
	}
}
