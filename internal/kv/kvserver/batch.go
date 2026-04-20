package kvserver

import (
	"context"
	"errors"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

type BatchHandler struct {
	engine storage.Engine
	clock  *hlc.Clock
}

func NewBatchHandler(engine storage.Engine, clock *hlc.Clock) *BatchHandler {
	return &BatchHandler{engine: engine, clock: clock}
}

func (bh *BatchHandler) Batch(_ context.Context, req *pb.BatchRequest) (*pb.BatchResponse, error) {
	resp := &pb.BatchResponse{}

	header := req.Header
	if header == nil {
		header = &pb.Header{}
	}

	for _, ru := range req.Requests {
		r, err := bh.executeSingle(header, ru)
		if err != nil {
			resp.Error = &pb.Error{Message: err.Error()}
			return resp, nil
		}
		resp.Responses = append(resp.Responses, r)
	}

	return resp, nil
}

func (bh *BatchHandler) executeSingle(header *pb.Header, req *pb.RequestUnion) (*pb.ResponseUnion, error) {
	ts := pb.ToHLC(header.Timestamp)
	var txn *mvcc.TxnID
	if header.Txn != nil && len(header.Txn.Id) > 0 {
		id := pb.ToTxnID(header.Txn.Id)
		txn = &id
	}

	readOpts := mvcc.ReadOptions{Txn: txn}
	if header.Txn != nil && header.Txn.MaxTimestamp != nil {
		readOpts.MaxTimestamp = pb.ToHLC(header.Txn.MaxTimestamp)
	}

	switch v := req.Value.(type) {
	case *pb.RequestUnion_Get:
		val, err := mvcc.MVCCGet(bh.engine, v.Get.Key, ts, readOpts)
		if errors.Is(err, storage.ErrKeyNotFound) {
			return &pb.ResponseUnion{Value: &pb.ResponseUnion_Get{Get: &pb.GetResponse{}}}, nil
		}
		if err != nil {
			return nil, err
		}
		return &pb.ResponseUnion{Value: &pb.ResponseUnion_Get{Get: &pb.GetResponse{
			Value: &pb.Value{RawBytes: val, Timestamp: header.Timestamp},
		}}}, nil

	case *pb.RequestUnion_Put:
		var raw []byte
		if v.Put.Value != nil {
			raw = v.Put.Value.RawBytes
		}
		if err := mvcc.MVCCPut(bh.engine, v.Put.Key, ts, raw, txn); err != nil {
			return nil, err
		}
		return &pb.ResponseUnion{Value: &pb.ResponseUnion_Put{Put: &pb.PutResponse{}}}, nil

	case *pb.RequestUnion_Delete:
		if err := mvcc.MVCCDelete(bh.engine, v.Delete.Key, ts, txn); err != nil {
			return nil, err
		}
		return &pb.ResponseUnion{Value: &pb.ResponseUnion_Delete{Delete: &pb.DeleteResponse{}}}, nil

	case *pb.RequestUnion_Scan:
		kvs, err := mvcc.MVCCScan(bh.engine, v.Scan.StartKey, v.Scan.EndKey, ts, readOpts)
		if err != nil {
			return nil, err
		}
		rows := make([]*pb.KeyValue, len(kvs))
		for i, kv := range kvs {
			rows[i] = &pb.KeyValue{
				Key:   kv.Key,
				Value: &pb.Value{RawBytes: kv.Value, Timestamp: header.Timestamp},
			}
		}
		if v.Scan.MaxKeys > 0 && int64(len(rows)) > v.Scan.MaxKeys {
			rows = rows[:v.Scan.MaxKeys]
		}
		return &pb.ResponseUnion{Value: &pb.ResponseUnion_Scan{Scan: &pb.ScanResponse{Rows: rows}}}, nil

	default:
		return nil, errors.New("unknown request type")
	}
}
