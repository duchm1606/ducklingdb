package executor_test

import (
	"strings"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/sql/executor"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func newTestEngine(t *testing.T) (storage.Engine, *hlc.Clock) {
	t.Helper()
	eng, err := lsm.OpenLSM(lsm.LSMOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	clock := hlc.NewClock(hlc.SystemWallClock(), 0)
	return eng, clock
}

func newExec(t *testing.T) *executor.Executor {
	t.Helper()
	eng, clock := newTestEngine(t)
	return executor.New(eng, clock)
}

func mustExec(t *testing.T, ex *executor.Executor, sql string) *executor.Result {
	t.Helper()
	res, err := ex.Execute(sql)
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	return res
}

func TestCreateTable(t *testing.T) {
	ex := newExec(t)
	res := mustExec(t, ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if res.Message != "OK" {
		t.Fatalf("expected OK, got %q", res.Message)
	}
}

func TestCreateTableDuplicate(t *testing.T) {
	ex := newExec(t)
	mustExec(t, ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	_, err := ex.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err == nil {
		t.Fatal("expected error for duplicate table")
	}
}

func TestInsertAndSelectAll(t *testing.T) {
	ex := newExec(t)
	mustExec(t, ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, ex, "INSERT INTO users VALUES (1, 'alice')")
	mustExec(t, ex, "INSERT INTO users VALUES (2, 'bob')")

	res := mustExec(t, ex, "SELECT * FROM users")
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
}

func TestSelectWherePK(t *testing.T) {
	ex := newExec(t)
	mustExec(t, ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, ex, "INSERT INTO users VALUES (1, 'alice')")
	mustExec(t, ex, "INSERT INTO users VALUES (2, 'bob')")

	res := mustExec(t, ex, "SELECT * FROM users WHERE id = 1")
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if !strings.Contains(res.Rows[0][1], "alice") {
		t.Fatalf("expected alice, got %v", res.Rows[0])
	}
}

func TestUpdate(t *testing.T) {
	ex := newExec(t)
	mustExec(t, ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, ex, "INSERT INTO users VALUES (1, 'alice')")
	mustExec(t, ex, "UPDATE users SET name = 'alice2' WHERE id = 1")

	res := mustExec(t, ex, "SELECT * FROM users WHERE id = 1")
	if len(res.Rows) != 1 || !strings.Contains(res.Rows[0][1], "alice2") {
		t.Fatalf("expected alice2, got %v", res.Rows)
	}
}

func TestDelete(t *testing.T) {
	ex := newExec(t)
	mustExec(t, ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, ex, "INSERT INTO users VALUES (1, 'alice')")
	mustExec(t, ex, "DELETE FROM users WHERE id = 1")

	res := mustExec(t, ex, "SELECT * FROM users")
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", len(res.Rows))
	}
}

func TestSelectNotFound(t *testing.T) {
	ex := newExec(t)
	mustExec(t, ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	res := mustExec(t, ex, "SELECT * FROM users WHERE id = 99")
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(res.Rows))
	}
}

func TestWhereNonPKError(t *testing.T) {
	ex := newExec(t)
	mustExec(t, ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	_, err := ex.Execute("SELECT * FROM users WHERE name = 'alice'")
	if err == nil {
		t.Fatal("expected error for non-PK WHERE")
	}
	if !strings.Contains(err.Error(), "WHERE only supported on primary key column") {
		t.Fatalf("expected WHERE error message, got: %v", err)
	}
}

func TestCreateTableInvalidPKType(t *testing.T) {
	eng, clock := newTestEngine(t)
	ex := executor.New(eng, clock)

	_, err := ex.Execute("CREATE TABLE t (id FLOAT PRIMARY KEY, name TEXT)")
	if err == nil {
		t.Fatal("expected error for FLOAT primary key, got nil")
	}

	_, err = ex.Execute("CREATE TABLE t2 (flag BOOL PRIMARY KEY)")
	if err == nil {
		t.Fatal("expected error for BOOL primary key, got nil")
	}
}
