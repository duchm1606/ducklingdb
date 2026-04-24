package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/storage"
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

// PrimaryKeyColumn returns the primary key column, or nil if none.
func (s *TableSchema) PrimaryKeyColumn() *Column {
	for i := range s.Columns {
		if s.Columns[i].PrimaryKey {
			return &s.Columns[i]
		}
	}
	return nil
}

func schemaKey(name string) []byte {
	return []byte("\x00schema/" + name)
}

func CreateTable(engine storage.Engine, schema *TableSchema) error {
	if _, err := engine.Get(schemaKey(schema.Name)); err == nil {
		return fmt.Errorf("table %q already exists", schema.Name)
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return engine.Put(schemaKey(schema.Name), data)
}

func GetTable(engine storage.Engine, name string) (*TableSchema, error) {
	data, err := engine.Get(schemaKey(name))
	if err != nil {
		return nil, fmt.Errorf("table %q not found", name)
	}
	var schema TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

func DropTable(engine storage.Engine, name string) error {
	if _, err := engine.Get(schemaKey(name)); err != nil {
		return fmt.Errorf("table %q not found", name)
	}
	return engine.Delete(schemaKey(name))
}

func ListTables(engine storage.Engine) ([]*TableSchema, error) {
	iter, err := engine.NewIterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	start := []byte("\x00schema/")
	end := []byte("\x00schema/\xff")
	var out []*TableSchema
	for iter.Seek(start); iter.Valid(); iter.Next() {
		if bytes.Compare(iter.Key(), end) >= 0 {
			break
		}
		if iter.IsTombstone() {
			continue
		}
		var s TableSchema
		if err := json.Unmarshal(iter.Value(), &s); err != nil {
			return nil, err
		}
		out = append(out, &s)
	}
	return out, nil
}
