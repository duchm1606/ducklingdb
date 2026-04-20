package kvserver

import (
	"context"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func setup(t *testing.T) *BatchHandler {
	t.Helper()
	engine, err := lsm.OpenLSM(lsm.LSMOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenLSM: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	clock := hlc.NewClock(hlc.NewManualClock(1000), 500*time.Millisecond)
	return NewBatchHandler(engine, clock)
}

func ts(wall int64, logical int32) *pb.Timestamp {
	return &pb.Timestamp{WallTime: wall, Logical: logical}
}

func TestBatchPutThenGet(t *testing.T) {
	bh := setup(t)
	ctx := context.Background()

	resp, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(100, 0)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("hello"),
				Value: &pb.Value{RawBytes: []byte("world")},
			}}},
			{Value: &pb.RequestUnion_Get{Get: &pb.GetRequest{
				Key: []byte("hello"),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("batch error: %s", resp.Error.Message)
	}
	if len(resp.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(resp.Responses))
	}

	getResp := resp.Responses[1].GetGet()
	if getResp == nil || getResp.Value == nil {
		t.Fatal("get response is nil")
	}
	if string(getResp.Value.RawBytes) != "world" {
		t.Fatalf("got %q, want %q", getResp.Value.RawBytes, "world")
	}
}

func TestBatchDelete(t *testing.T) {
	bh := setup(t)
	ctx := context.Background()

	_, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(100, 0)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("k1"),
				Value: &pb.Value{RawBytes: []byte("v1")},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Put batch: %v", err)
	}

	_, err = bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(200, 0)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Delete{Delete: &pb.DeleteRequest{
				Key: []byte("k1"),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Delete batch: %v", err)
	}

	resp, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(200, 1)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Get{Get: &pb.GetRequest{
				Key: []byte("k1"),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Get batch: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("batch error: %s", resp.Error.Message)
	}

	getResp := resp.Responses[0].GetGet()
	if getResp == nil {
		t.Fatal("get response is nil")
	}
	if getResp.Value != nil && len(getResp.Value.RawBytes) > 0 {
		t.Fatalf("expected empty value after delete, got %q", getResp.Value.RawBytes)
	}
}

func TestBatchScan(t *testing.T) {
	bh := setup(t)
	ctx := context.Background()

	_, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(100, 0)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("a"),
				Value: &pb.Value{RawBytes: []byte("1")},
			}}},
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("b"),
				Value: &pb.Value{RawBytes: []byte("2")},
			}}},
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("c"),
				Value: &pb.Value{RawBytes: []byte("3")},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Put batch: %v", err)
	}

	resp, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(100, 1)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Scan{Scan: &pb.ScanRequest{
				StartKey: []byte("a"),
				EndKey:   []byte("c"),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Scan batch: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("batch error: %s", resp.Error.Message)
	}

	scanResp := resp.Responses[0].GetScan()
	if scanResp == nil {
		t.Fatal("scan response is nil")
	}
	if len(scanResp.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(scanResp.Rows))
	}
	if string(scanResp.Rows[0].Key) != "a" || string(scanResp.Rows[1].Key) != "b" {
		t.Fatalf("unexpected keys: %q, %q", scanResp.Rows[0].Key, scanResp.Rows[1].Key)
	}
}

func TestBatchScanMaxKeys(t *testing.T) {
	bh := setup(t)
	ctx := context.Background()

	_, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(100, 0)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("a"),
				Value: &pb.Value{RawBytes: []byte("1")},
			}}},
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("b"),
				Value: &pb.Value{RawBytes: []byte("2")},
			}}},
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("c"),
				Value: &pb.Value{RawBytes: []byte("3")},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Put batch: %v", err)
	}

	resp, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(100, 1)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Scan{Scan: &pb.ScanRequest{
				StartKey: []byte("a"),
				EndKey:   []byte("d"),
				MaxKeys:  2,
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Scan batch: %v", err)
	}

	scanResp := resp.Responses[0].GetScan()
	if len(scanResp.Rows) != 2 {
		t.Fatalf("expected 2 rows (max_keys=2), got %d", len(scanResp.Rows))
	}
}

func TestBatchEmpty(t *testing.T) {
	bh := setup(t)
	ctx := context.Background()

	resp, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(100, 0)},
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("batch error: %s", resp.Error.Message)
	}
	if len(resp.Responses) != 0 {
		t.Fatalf("expected 0 responses, got %d", len(resp.Responses))
	}
}

func TestBatchErrorStopsExecution(t *testing.T) {
	bh := setup(t)
	ctx := context.Background()

	_, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(100, 0)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("k"),
				Value: &pb.Value{RawBytes: []byte("v")},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("initial Put: %v", err)
	}

	resp, err := bh.Batch(ctx, &pb.BatchRequest{
		Header: &pb.Header{Timestamp: ts(50, 0)},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("k"),
				Value: &pb.Value{RawBytes: []byte("v2")},
			}}},
			{Value: &pb.RequestUnion_Get{Get: &pb.GetRequest{
				Key: []byte("k"),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected write-too-old error")
	}
	if len(resp.Responses) != 0 {
		t.Fatalf("expected 0 responses on error, got %d", len(resp.Responses))
	}
}
