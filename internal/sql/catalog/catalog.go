package catalog

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

type ColumnType string

const (
	TypeINT   ColumnType = "INT"
	TypeTEXT  ColumnType = "TEXT"
	TypeBOOL  ColumnType = "BOOL"
	TypeFLOAT ColumnType = "FLOAT"
)

type Column struct {
	Name       string     `json:"name"`
	Type       ColumnType `json:"type"`
	PrimaryKey bool       `json:"primary_key"`
}

type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

func (s *TableSchema) PrimaryKeyColumn() *Column {
	for i := range s.Columns {
		if s.Columns[i].PrimaryKey {
			return &s.Columns[i]
		}
	}
	return nil
}

// SchemaKey returns the MVCC key used to store the schema for the named table.
func SchemaKey(name string) []byte {
	return []byte("\x00schema/" + name)
}

// maxSchemaTS is used for reads — always returns the latest schema version.
var maxSchemaTS = hlc.Timestamp{WallTime: math.MaxInt64}

func CreateTable(engine storage.Engine, clock *hlc.Clock, schema *TableSchema) error {
	val, _ := mvcc.MVCCGet(engine, SchemaKey(schema.Name), maxSchemaTS, mvcc.ReadOptions{})
	if val != nil {
		return fmt.Errorf("table %q already exists", schema.Name)
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return mvcc.MVCCPut(engine, SchemaKey(schema.Name), clock.Now(), data, nil)
}

func GetTable(engine storage.Engine, name string) (*TableSchema, error) {
	val, err := mvcc.MVCCGet(engine, SchemaKey(name), maxSchemaTS, mvcc.ReadOptions{})
	if err != nil || val == nil {
		return nil, fmt.Errorf("table %q not found", name)
	}
	var schema TableSchema
	if err := json.Unmarshal(val, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

func DropTable(engine storage.Engine, clock *hlc.Clock, name string) error {
	val, _ := mvcc.MVCCGet(engine, SchemaKey(name), maxSchemaTS, mvcc.ReadOptions{})
	if val == nil {
		return fmt.Errorf("table %q not found", name)
	}
	return mvcc.MVCCDelete(engine, SchemaKey(name), clock.Now(), nil)
}

func ListTables(engine storage.Engine) ([]*TableSchema, error) {
	kvs, err := mvcc.MVCCScan(engine,
		[]byte("\x00schema/"),
		[]byte("\x00schema0"),
		maxSchemaTS,
		mvcc.ReadOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*TableSchema, 0, len(kvs))
	for _, kv := range kvs {
		var s TableSchema
		if err := json.Unmarshal(kv.Value, &s); err != nil {
			return nil, err
		}
		out = append(out, &s)
	}
	return out, nil
}
