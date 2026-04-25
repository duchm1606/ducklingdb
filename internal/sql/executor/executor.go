package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/sql/catalog"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// BatchSender routes a write batch. When nil, writes go directly to the
// local engine. When set, the batch is sent via Raft (or any transport).
type BatchSender func(ctx context.Context, req *pb.BatchRequest) (*pb.BatchResponse, error)

// Result holds the output of a SQL statement.
type Result struct {
	Columns      []string
	Rows         [][]string
	RowsAffected int
	Message      string
}

// Executor runs SQL statements against the MVCC engine.
type Executor struct {
	engine storage.Engine
	clock  *hlc.Clock
	sender BatchSender // nil = write directly to engine
}

func New(engine storage.Engine, clock *hlc.Clock) *Executor {
	return &Executor{engine: engine, clock: clock}
}

// NewWithSender creates an executor that routes write operations through sender.
// Reads (SELECT) always use the local engine.
func NewWithSender(engine storage.Engine, clock *hlc.Clock, sender BatchSender) *Executor {
	return &Executor{engine: engine, clock: clock, sender: sender}
}

func (e *Executor) Execute(sql string) (*Result, error) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if len(stmts) == 0 {
		return &Result{Message: "OK"}, nil
	}
	switch s := stmts[0].AST.(type) {
	case *tree.CreateTable:
		return e.execCreateTable(s)
	case *tree.Insert:
		return e.execInsert(s)
	case *tree.Select:
		return e.execSelect(s)
	case *tree.Update:
		return e.execUpdate(s)
	case *tree.Delete:
		return e.execDelete(s)
	default:
		return nil, fmt.Errorf("unsupported statement: %T", stmts[0].AST)
	}
}

func (e *Executor) execCreateTable(s *tree.CreateTable) (*Result, error) {
	schema := &catalog.TableSchema{Name: s.Table.Table()}
	for _, def := range s.Defs {
		col, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		ct, err := mapColumnType(col.Type.SQLString())
		if err != nil {
			return nil, err
		}
		isPK := bool(col.PrimaryKey.IsPrimaryKey)
		if isPK && ct != catalog.TypeINT && ct != catalog.TypeTEXT {
			return nil, fmt.Errorf("primary key column %q must be INT or TEXT", col.Name)
		}
		schema.Columns = append(schema.Columns, catalog.Column{
			Name:       string(col.Name),
			Type:       ct,
			PrimaryKey: isPK,
		})
	}
	if schema.PrimaryKeyColumn() == nil {
		return nil, fmt.Errorf("table must have a PRIMARY KEY column")
	}

	if e.sender != nil {
		// Check duplicate locally before proposing.
		if _, err := catalog.GetTable(e.engine, schema.Name); err == nil {
			return nil, fmt.Errorf("table %q already exists", schema.Name)
		}
		data, err := json.Marshal(schema)
		if err != nil {
			return nil, err
		}
		ts := e.clock.Now()
		req := &pb.BatchRequest{
			Header: &pb.Header{Timestamp: pb.FromHLC(ts)},
			Requests: []*pb.RequestUnion{{
				Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
					Key:   catalog.SchemaKey(schema.Name),
					Value: &pb.Value{RawBytes: data},
				}},
			}},
		}
		if _, err := e.sender(context.Background(), req); err != nil {
			return nil, err
		}
		return &Result{Message: "OK"}, nil
	}

	if err := catalog.CreateTable(e.engine, e.clock, schema); err != nil {
		return nil, err
	}
	return &Result{Message: "OK"}, nil
}

func (e *Executor) execInsert(s *tree.Insert) (*Result, error) {
	tableName := tableNameFromExpr(s.Table)
	schema, err := catalog.GetTable(e.engine, tableName)
	if err != nil {
		return nil, err
	}
	pk := schema.PrimaryKeyColumn()
	if pk == nil {
		return nil, fmt.Errorf("table %q has no primary key", tableName)
	}
	values, ok := s.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return nil, fmt.Errorf("INSERT: only VALUES clause supported")
	}
	ts := e.clock.Now()
	var requests []*pb.RequestUnion
	count := 0
	for _, rowExprs := range values.Rows {
		if len(rowExprs) != len(schema.Columns) {
			return nil, fmt.Errorf("INSERT: expected %d values, got %d", len(schema.Columns), len(rowExprs))
		}
		row := make(map[string]any, len(schema.Columns))
		var pkVal any
		for i, col := range schema.Columns {
			val, err := exprToValue(rowExprs[i])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
			row[col.Name] = val
			if col.PrimaryKey {
				pkVal = val
			}
		}
		key := encodeRowKey(tableName, pk.Type, pkVal)
		valBytes, err := encodeRowValue(schema.Columns, row)
		if err != nil {
			return nil, err
		}
		requests = append(requests, &pb.RequestUnion{
			Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   key,
				Value: &pb.Value{RawBytes: valBytes},
			}},
		})
		count++
	}

	if e.sender != nil {
		req := &pb.BatchRequest{
			Header:   &pb.Header{Timestamp: pb.FromHLC(ts)},
			Requests: requests,
		}
		if _, err := e.sender(context.Background(), req); err != nil {
			return nil, err
		}
		return &Result{RowsAffected: count, Message: fmt.Sprintf("INSERT %d", count)}, nil
	}

	// Direct path: write to MVCC.
	for _, ru := range requests {
		put := ru.Value.(*pb.RequestUnion_Put).Put
		if err := mvcc.MVCCPut(e.engine, put.Key, ts, put.Value.RawBytes, nil); err != nil {
			return nil, err
		}
	}
	return &Result{RowsAffected: count, Message: fmt.Sprintf("INSERT %d", count)}, nil
}

func (e *Executor) execSelect(s *tree.Select) (*Result, error) {
	sc, ok := s.Select.(*tree.SelectClause)
	if !ok {
		return nil, fmt.Errorf("SELECT: unsupported form")
	}
	if len(sc.From.Tables) == 0 {
		return nil, fmt.Errorf("SELECT: no table specified")
	}
	tableName := tableNameFromExpr(sc.From.Tables[0])
	schema, err := catalog.GetTable(e.engine, tableName)
	if err != nil {
		return nil, err
	}
	ts := e.clock.Now()
	opts := mvcc.ReadOptions{}
	var kvPairs []mvcc.KeyValue
	if sc.Where != nil {
		pkVal, err := whereExprToPKValue(sc.Where.Expr, schema)
		if err != nil {
			return nil, err
		}
		pk := schema.PrimaryKeyColumn()
		key := encodeRowKey(tableName, pk.Type, pkVal)
		val, err := mvcc.MVCCGet(e.engine, key, ts, opts)
		if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			return nil, err
		}
		if val != nil {
			kvPairs = []mvcc.KeyValue{{Key: key, Value: val}}
		}
	} else {
		start, end := tableKeyRange(tableName)
		kvPairs, err = mvcc.MVCCScan(e.engine, start, end, ts, opts)
		if err != nil {
			return nil, err
		}
	}
	return buildResult(schema, kvPairs)
}

func (e *Executor) execUpdate(s *tree.Update) (*Result, error) {
	tableName := tableNameFromExpr(s.Table)
	schema, err := catalog.GetTable(e.engine, tableName)
	if err != nil {
		return nil, err
	}
	if s.Where == nil {
		return nil, fmt.Errorf("UPDATE: WHERE clause required")
	}
	pkVal, err := whereExprToPKValue(s.Where.Expr, schema)
	if err != nil {
		return nil, err
	}
	pk := schema.PrimaryKeyColumn()
	key := encodeRowKey(tableName, pk.Type, pkVal)
	ts := e.clock.Now()
	existing, err := mvcc.MVCCGet(e.engine, key, ts, mvcc.ReadOptions{})
	if errors.Is(err, storage.ErrKeyNotFound) || existing == nil {
		return &Result{RowsAffected: 0, Message: "UPDATE 0"}, nil
	}
	if err != nil {
		return nil, err
	}
	row, err := decodeRowValue(existing)
	if err != nil {
		return nil, err
	}
	for _, expr := range s.Exprs {
		colName := string(expr.Names[0])
		val, err := exprToValue(expr.Expr)
		if err != nil {
			return nil, fmt.Errorf("SET %q: %w", colName, err)
		}
		row[colName] = val
	}
	valBytes, err := encodeRowValue(schema.Columns, row)
	if err != nil {
		return nil, err
	}

	if e.sender != nil {
		newTs := e.clock.Now()
		req := &pb.BatchRequest{
			Header: &pb.Header{Timestamp: pb.FromHLC(newTs)},
			Requests: []*pb.RequestUnion{{
				Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
					Key:   key,
					Value: &pb.Value{RawBytes: valBytes},
				}},
			}},
		}
		if _, err := e.sender(context.Background(), req); err != nil {
			return nil, err
		}
		return &Result{RowsAffected: 1, Message: "UPDATE 1"}, nil
	}

	newTs := e.clock.Now()
	if err := mvcc.MVCCPut(e.engine, key, newTs, valBytes, nil); err != nil {
		return nil, err
	}
	return &Result{RowsAffected: 1, Message: "UPDATE 1"}, nil
}

func (e *Executor) execDelete(s *tree.Delete) (*Result, error) {
	tableName := tableNameFromExpr(s.Table)
	schema, err := catalog.GetTable(e.engine, tableName)
	if err != nil {
		return nil, err
	}
	if s.Where == nil {
		return nil, fmt.Errorf("DELETE: WHERE clause required")
	}
	pkVal, err := whereExprToPKValue(s.Where.Expr, schema)
	if err != nil {
		return nil, err
	}
	pk := schema.PrimaryKeyColumn()
	key := encodeRowKey(tableName, pk.Type, pkVal)
	ts := e.clock.Now()
	existing, err := mvcc.MVCCGet(e.engine, key, ts, mvcc.ReadOptions{})
	if errors.Is(err, storage.ErrKeyNotFound) || existing == nil {
		return &Result{RowsAffected: 0, Message: "DELETE 0"}, nil
	}
	if err != nil {
		return nil, err
	}

	if e.sender != nil {
		req := &pb.BatchRequest{
			Header: &pb.Header{Timestamp: pb.FromHLC(ts)},
			Requests: []*pb.RequestUnion{{
				Value: &pb.RequestUnion_Delete{Delete: &pb.DeleteRequest{Key: key}},
			}},
		}
		if _, err := e.sender(context.Background(), req); err != nil {
			return nil, err
		}
		return &Result{RowsAffected: 1, Message: "DELETE 1"}, nil
	}

	if err := mvcc.MVCCDelete(e.engine, key, ts, nil); err != nil {
		return nil, err
	}
	return &Result{RowsAffected: 1, Message: "DELETE 1"}, nil
}

func buildResult(schema *catalog.TableSchema, kvPairs []mvcc.KeyValue) (*Result, error) {
	cols := make([]string, len(schema.Columns))
	for i, c := range schema.Columns {
		cols[i] = c.Name
	}
	res := &Result{Columns: cols}
	pk := schema.PrimaryKeyColumn()
	for _, kv := range kvPairs {
		rowMap, err := decodeRowValue(kv.Value)
		if err != nil {
			return nil, err
		}
		pkStr := decodePKFromKey(kv.Key, schema.Name, pk.Type)
		row := make([]string, len(schema.Columns))
		for i, col := range schema.Columns {
			if col.PrimaryKey {
				row[i] = pkStr
			} else {
				row[i] = fmt.Sprintf("%v", rowMap[col.Name])
			}
		}
		res.Rows = append(res.Rows, row)
	}
	if len(res.Rows) == 1 {
		res.Message = "(1 row)"
	} else {
		res.Message = fmt.Sprintf("(%d rows)", len(res.Rows))
	}
	return res, nil
}

func whereExprToPKValue(expr tree.Expr, schema *catalog.TableSchema) (any, error) {
	cmp, ok := expr.(*tree.ComparisonExpr)
	if !ok {
		return nil, fmt.Errorf("WHERE only supported on primary key column")
	}
	if cmp.Operator != tree.EQ {
		return nil, fmt.Errorf("WHERE only supported on primary key column")
	}
	colName, ok := cmp.Left.(*tree.UnresolvedName)
	if !ok {
		return nil, fmt.Errorf("WHERE: left side must be a column name")
	}
	pk := schema.PrimaryKeyColumn()
	if pk == nil || string(colName.Parts[0]) != pk.Name {
		return nil, fmt.Errorf("WHERE only supported on primary key column")
	}
	return exprToValue(cmp.Right)
}

func exprToValue(expr tree.Expr) (any, error) {
	switch v := expr.(type) {
	case *tree.NumVal:
		if strings.Contains(v.OrigString(), ".") {
			return strconv.ParseFloat(v.OrigString(), 64)
		}
		return strconv.ParseInt(v.OrigString(), 10, 64)
	case *tree.StrVal:
		return v.RawString(), nil
	case *tree.DBool:
		return bool(*v), nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", expr)
	}
}

func tableNameFromExpr(expr tree.TableExpr) string {
	switch t := expr.(type) {
	case *tree.AliasedTableExpr:
		return tableNameFromExpr(t.Expr)
	case *tree.TableName:
		return t.Table()
	default:
		return fmt.Sprintf("%v", expr)
	}
}

func mapColumnType(s string) (catalog.ColumnType, error) {
	switch strings.ToUpper(s) {
	case "INT", "INT8", "INT4", "INTEGER", "BIGINT":
		return catalog.TypeINT, nil
	case "TEXT", "STRING", "VARCHAR", "CHAR":
		return catalog.TypeTEXT, nil
	case "BOOL", "BOOLEAN":
		return catalog.TypeBOOL, nil
	case "FLOAT", "FLOAT8", "FLOAT4", "REAL", "DOUBLE PRECISION":
		return catalog.TypeFLOAT, nil
	default:
		return "", fmt.Errorf("unsupported column type: %s", s)
	}
}
