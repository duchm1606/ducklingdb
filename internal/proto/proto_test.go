package proto

import (
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
	pb "google.golang.org/protobuf/proto"
)

func TestTimestampConversion(t *testing.T) {
	original := hlc.Timestamp{WallTime: 1713500000000000000, Logical: 42}

	protoTS := FromHLC(original)
	if protoTS.WallTime != original.WallTime || protoTS.Logical != original.Logical {
		t.Fatalf("FromHLC: got {%d, %d}, want {%d, %d}",
			protoTS.WallTime, protoTS.Logical, original.WallTime, original.Logical)
	}

	roundTripped := ToHLC(protoTS)
	if roundTripped != original {
		t.Fatalf("round-trip: got %v, want %v", roundTripped, original)
	}

	if got := ToHLC(nil); !got.IsEmpty() {
		t.Fatalf("ToHLC(nil): got %v, want zero", got)
	}
}

func TestTxnIDConversion(t *testing.T) {
	var original mvcc.TxnID
	for i := range original {
		original[i] = byte(i + 1)
	}

	protoBytes := FromTxnID(original)
	if len(protoBytes) != 16 {
		t.Fatalf("FromTxnID: got len %d, want 16", len(protoBytes))
	}

	roundTripped := ToTxnID(protoBytes)
	if roundTripped != original {
		t.Fatalf("round-trip: got %v, want %v", roundTripped, original)
	}
}

func TestBatchRequestMarshalRoundTrip(t *testing.T) {
	req := &BatchRequest{
		Header: &Header{
			Timestamp: FromHLC(hlc.Timestamp{WallTime: 100, Logical: 1}),
		},
		Requests: []*RequestUnion{
			{Value: &RequestUnion_Put{Put: &PutRequest{
				Key:   []byte("hello"),
				Value: &Value{RawBytes: []byte("world")},
			}}},
			{Value: &RequestUnion_Get{Get: &GetRequest{
				Key: []byte("hello"),
			}}},
			{Value: &RequestUnion_Delete{Delete: &DeleteRequest{
				Key: []byte("goodbye"),
			}}},
			{Value: &RequestUnion_Scan{Scan: &ScanRequest{
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				MaxKeys:  100,
			}}},
		},
	}

	data, err := pb.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded BatchRequest
	if err := pb.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(decoded.Requests) != 4 {
		t.Fatalf("requests count: got %d, want 4", len(decoded.Requests))
	}

	put := decoded.Requests[0].GetPut()
	if put == nil {
		t.Fatal("requests[0] is not a PutRequest")
	}
	if string(put.Key) != "hello" || string(put.Value.RawBytes) != "world" {
		t.Fatalf("put: got key=%q val=%q, want hello/world", put.Key, put.Value.RawBytes)
	}

	get := decoded.Requests[1].GetGet()
	if get == nil {
		t.Fatal("requests[1] is not a GetRequest")
	}
	if string(get.Key) != "hello" {
		t.Fatalf("get: got key=%q, want hello", get.Key)
	}

	del := decoded.Requests[2].GetDelete()
	if del == nil {
		t.Fatal("requests[2] is not a DeleteRequest")
	}

	scan := decoded.Requests[3].GetScan()
	if scan == nil {
		t.Fatal("requests[3] is not a ScanRequest")
	}
	if scan.MaxKeys != 100 {
		t.Fatalf("scan max_keys: got %d, want 100", scan.MaxKeys)
	}
}

func TestBatchResponseMarshalRoundTrip(t *testing.T) {
	resp := &BatchResponse{
		Responses: []*ResponseUnion{
			{Value: &ResponseUnion_Put{Put: &PutResponse{}}},
			{Value: &ResponseUnion_Get{Get: &GetResponse{
				Value: &Value{
					RawBytes:  []byte("world"),
					Timestamp: FromHLC(hlc.Timestamp{WallTime: 100, Logical: 1}),
				},
			}}},
		},
	}

	data, err := pb.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded BatchResponse
	if err := pb.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(decoded.Responses) != 2 {
		t.Fatalf("responses count: got %d, want 2", len(decoded.Responses))
	}

	getResp := decoded.Responses[1].GetGet()
	if getResp == nil {
		t.Fatal("responses[1] is not a GetResponse")
	}
	if string(getResp.Value.RawBytes) != "world" {
		t.Fatalf("get value: got %q, want world", getResp.Value.RawBytes)
	}

	ts := ToHLC(getResp.Value.Timestamp)
	if ts.WallTime != 100 || ts.Logical != 1 {
		t.Fatalf("get timestamp: got %v, want {100, 1}", ts)
	}
}

func TestBatchResponseWithError(t *testing.T) {
	resp := &BatchResponse{
		Error: &Error{Message: "key not found"},
	}

	data, err := pb.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded BatchResponse
	if err := pb.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Error == nil || decoded.Error.Message != "key not found" {
		t.Fatalf("error: got %v, want 'key not found'", decoded.Error)
	}
}

func TestNodeDescriptorMarshalRoundTrip(t *testing.T) {
	desc := &NodeDescriptor{
		NodeId:  1,
		Address: "localhost:26257",
		Attrs:   map[string]string{"region": "us-east", "zone": "us-east-1a"},
	}

	data, err := pb.Marshal(desc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded NodeDescriptor
	if err := pb.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.NodeId != 1 {
		t.Fatalf("node_id: got %d, want 1", decoded.NodeId)
	}
	if decoded.Address != "localhost:26257" {
		t.Fatalf("address: got %q, want localhost:26257", decoded.Address)
	}
	if decoded.Attrs["region"] != "us-east" || decoded.Attrs["zone"] != "us-east-1a" {
		t.Fatalf("attrs: got %v, want region=us-east, zone=us-east-1a", decoded.Attrs)
	}
}

func TestStoreDescriptorMarshalRoundTrip(t *testing.T) {
	desc := &StoreDescriptor{
		StoreId: 1,
		NodeId:  1,
		Capacity: &StoreCapacity{
			TotalBytes: 1 << 30,
			UsedBytes:  500 << 20,
			RangeCount: 42,
		},
	}

	data, err := pb.Marshal(desc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded StoreDescriptor
	if err := pb.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Capacity.RangeCount != 42 {
		t.Fatalf("range_count: got %d, want 42", decoded.Capacity.RangeCount)
	}
	if decoded.Capacity.TotalBytes != 1<<30 {
		t.Fatalf("total_bytes: got %d, want %d", decoded.Capacity.TotalBytes, 1<<30)
	}
}

func TestTxnMetaMarshalRoundTrip(t *testing.T) {
	var txnID mvcc.TxnID
	for i := range txnID {
		txnID[i] = byte(0xAB)
	}

	meta := &TxnMeta{
		Id:             FromTxnID(txnID),
		Status:         0, // PENDING
		Isolation:      0, // SSI
		ReadTimestamp:   FromHLC(hlc.Timestamp{WallTime: 100, Logical: 0}),
		WriteTimestamp:  FromHLC(hlc.Timestamp{WallTime: 200, Logical: 5}),
		MaxTimestamp:    FromHLC(hlc.Timestamp{WallTime: 600, Logical: 0}),
		Priority:        42,
	}

	data, err := pb.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded TxnMeta
	if err := pb.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	gotID := ToTxnID(decoded.Id)
	if gotID != txnID {
		t.Fatalf("txn id: got %x, want %x", gotID, txnID)
	}
	if decoded.Priority != 42 {
		t.Fatalf("priority: got %d, want 42", decoded.Priority)
	}

	readTS := ToHLC(decoded.ReadTimestamp)
	if readTS.WallTime != 100 {
		t.Fatalf("read_ts: got %v, want wall=100", readTS)
	}

	writeTS := ToHLC(decoded.WriteTimestamp)
	if writeTS.WallTime != 200 || writeTS.Logical != 5 {
		t.Fatalf("write_ts: got %v, want {200, 5}", writeTS)
	}
}

func TestPingRequestMarshalRoundTrip(t *testing.T) {
	req := &PingRequest{
		NodeId:     3,
		ServerTime: FromHLC(hlc.Timestamp{WallTime: 999, Logical: 7}),
	}

	data, err := pb.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded PingRequest
	if err := pb.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.NodeId != 3 {
		t.Fatalf("node_id: got %d, want 3", decoded.NodeId)
	}

	ts := ToHLC(decoded.ServerTime)
	if ts.WallTime != 999 || ts.Logical != 7 {
		t.Fatalf("server_time: got %v, want {999, 7}", ts)
	}
}
