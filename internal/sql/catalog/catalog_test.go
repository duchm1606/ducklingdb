package catalog_test

import (
	"testing"

	"github.com/duchm1606/ducklingdb/internal/sql/catalog"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func openEngine(t *testing.T) *lsm.LSMEngine {
	t.Helper()
	eng, err := lsm.OpenLSM(lsm.LSMOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return eng
}

func newClock() *hlc.Clock {
	return hlc.NewClock(hlc.SystemWallClock(), 0)
}

func TestCreateAndGetTable(t *testing.T) {
	eng := openEngine(t)
	clock := newClock()
	schema := &catalog.TableSchema{
		Name: "users",
		Columns: []catalog.Column{
			{Name: "id",   Type: catalog.TypeINT,  PrimaryKey: true},
			{Name: "name", Type: catalog.TypeTEXT, PrimaryKey: false},
		},
	}
	if err := catalog.CreateTable(eng, clock, schema); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	got, err := catalog.GetTable(eng, "users")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	if got.Name != "users" || len(got.Columns) != 2 {
		t.Fatalf("unexpected schema: %+v", got)
	}
}

func TestCreateTableDuplicate(t *testing.T) {
	eng := openEngine(t)
	clock := newClock()
	schema := &catalog.TableSchema{
		Name:    "t",
		Columns: []catalog.Column{{Name: "id", Type: catalog.TypeINT, PrimaryKey: true}},
	}
	if err := catalog.CreateTable(eng, clock, schema); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := catalog.CreateTable(eng, clock, schema); err == nil {
		t.Fatal("expected error for duplicate table")
	}
}

func TestGetTableNotFound(t *testing.T) {
	eng := openEngine(t)
	if _, err := catalog.GetTable(eng, "noexist"); err == nil {
		t.Fatal("expected error for missing table")
	}
}

func TestDropTable(t *testing.T) {
	eng := openEngine(t)
	clock := newClock()
	schema := &catalog.TableSchema{
		Name:    "t",
		Columns: []catalog.Column{{Name: "id", Type: catalog.TypeINT, PrimaryKey: true}},
	}
	catalog.CreateTable(eng, clock, schema)
	if err := catalog.DropTable(eng, clock, "t"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if _, err := catalog.GetTable(eng, "t"); err == nil {
		t.Fatal("expected error after drop")
	}
}

func TestListTables(t *testing.T) {
	eng := openEngine(t)
	clock := newClock()
	for _, name := range []string{"a", "b", "c"} {
		catalog.CreateTable(eng, clock, &catalog.TableSchema{
			Name:    name,
			Columns: []catalog.Column{{Name: "id", Type: catalog.TypeINT, PrimaryKey: true}},
		})
	}
	tables, err := catalog.ListTables(eng)
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(tables))
	}
}

func TestPrimaryKeyColumn(t *testing.T) {
	eng := openEngine(t)
	clock := newClock()
	schema := &catalog.TableSchema{
		Name: "t",
		Columns: []catalog.Column{
			{Name: "id",   Type: catalog.TypeINT,  PrimaryKey: true},
			{Name: "name", Type: catalog.TypeTEXT, PrimaryKey: false},
		},
	}
	catalog.CreateTable(eng, clock, schema)
	got, _ := catalog.GetTable(eng, "t")
	pk := got.PrimaryKeyColumn()
	if pk == nil || pk.Name != "id" {
		t.Fatalf("expected pk column 'id', got %v", pk)
	}
}
