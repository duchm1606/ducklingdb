package gossip

import (
	"testing"
	"time"
)

func TestInfoStoreAddAndGet(t *testing.T) {
	is := NewInfoStore()

	info := is.AddInfo("node-desc:1", []byte("data"), 0, 1)
	if info.OrigStamp == 0 {
		t.Fatal("expected non-zero OrigStamp")
	}

	got, ok := is.GetInfo("node-desc:1")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(got.Value) != "data" {
		t.Fatalf("got %q, want %q", got.Value, "data")
	}
	if got.NodeID != 1 {
		t.Fatalf("NodeID: got %d, want 1", got.NodeID)
	}
}

func TestInfoStoreGetMissing(t *testing.T) {
	is := NewInfoStore()
	_, ok := is.GetInfo("nonexistent")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestInfoStoreGetExpired(t *testing.T) {
	is := NewInfoStore()
	info := is.AddInfo("k", []byte("v"), 1*time.Nanosecond, 1)
	// Force expiry by backdating CreatedAt.
	is.mu.Lock()
	info.CreatedAt = time.Now().Add(-1 * time.Second)
	is.mu.Unlock()

	_, ok := is.GetInfo("k")
	if ok {
		t.Fatal("expected expired entry to be invisible")
	}
}

func TestInfoStoreHighWaterUpdated(t *testing.T) {
	is := NewInfoStore()
	i1 := is.AddInfo("k1", []byte("v"), 0, 1)
	i2 := is.AddInfo("k2", []byte("v"), 0, 1)

	hw := is.HighWater()
	if hw[1] != i2.OrigStamp {
		t.Fatalf("high-water for node 1: got %d, want %d", hw[1], i2.OrigStamp)
	}
	_ = i1
}

func TestInfoStoreCombineAcceptsNew(t *testing.T) {
	is := NewInfoStore()

	accepted := is.Combine(&Info{
		Key:       "k1",
		Value:     []byte("v1"),
		OrigStamp: 5,
		NodeID:    2,
		CreatedAt: time.Now(),
	})
	if !accepted {
		t.Fatal("expected new info to be accepted")
	}

	got, ok := is.GetInfo("k1")
	if !ok || string(got.Value) != "v1" {
		t.Fatalf("expected stored value v1, got %q", got.GetValue())
	}
	if is.HighWater()[2] != 5 {
		t.Fatalf("high-water for node 2: got %d, want 5", is.HighWater()[2])
	}
}

func TestInfoStoreCombineRejectsStale(t *testing.T) {
	is := NewInfoStore()

	is.Combine(&Info{Key: "k1", Value: []byte("v1"), OrigStamp: 10, NodeID: 2, CreatedAt: time.Now()})

	accepted := is.Combine(&Info{Key: "k1", Value: []byte("v2"), OrigStamp: 3, NodeID: 2, CreatedAt: time.Now()})
	if accepted {
		t.Fatal("expected stale info to be rejected")
	}

	got, _ := is.GetInfo("k1")
	if string(got.Value) != "v1" {
		t.Fatalf("expected original value v1 preserved, got %q", got.Value)
	}
}

func TestInfoStoreCombineAcceptsFresher(t *testing.T) {
	is := NewInfoStore()
	is.Combine(&Info{Key: "k1", Value: []byte("old"), OrigStamp: 3, NodeID: 1, CreatedAt: time.Now()})
	is.Combine(&Info{Key: "k1", Value: []byte("new"), OrigStamp: 7, NodeID: 1, CreatedAt: time.Now()})

	got, _ := is.GetInfo("k1")
	if string(got.Value) != "new" {
		t.Fatalf("expected fresher value, got %q", got.Value)
	}
}

func TestInfoStoreDelta(t *testing.T) {
	is := NewInfoStore()

	// Three items: two from node 1, one from node 2.
	i1 := is.AddInfo("k1", []byte("v1"), 0, 1) // lower seq from node 1
	i2 := is.AddInfo("k2", []byte("v2"), 0, 1) // higher seq from node 1
	i3 := is.AddInfo("k3", []byte("v3"), 0, 2) // from node 2

	// Peer has seen up to i1.OrigStamp from node 1, nothing from node 2.
	delta := is.Delta(map[int32]int64{1: i1.OrigStamp})

	if _, ok := delta["k1"]; ok {
		t.Fatal("k1 should not be in delta (peer already has it)")
	}
	if _, ok := delta["k2"]; !ok {
		t.Fatal("k2 should be in delta (seq higher than peer's high-water for node 1)")
	}
	if _, ok := delta["k3"]; !ok {
		t.Fatal("k3 should be in delta (peer has no high-water for node 2)")
	}
	_ = i2
	_ = i3
}

func TestInfoStoreDeltaEmptyWhenSynced(t *testing.T) {
	is := NewInfoStore()
	i1 := is.AddInfo("k1", []byte("v"), 0, 1)

	delta := is.Delta(map[int32]int64{1: i1.OrigStamp})
	if len(delta) != 0 {
		t.Fatalf("expected empty delta when fully synced, got %d items", len(delta))
	}
}

func TestInfoStoreDeltaExcludesExpired(t *testing.T) {
	is := NewInfoStore()
	info := is.AddInfo("k", []byte("v"), 1*time.Nanosecond, 1)

	is.mu.Lock()
	info.CreatedAt = time.Now().Add(-1 * time.Second)
	is.mu.Unlock()

	delta := is.Delta(map[int32]int64{})
	if _, ok := delta["k"]; ok {
		t.Fatal("expired info should not appear in delta")
	}
}

func TestInfoStoreAllInfos(t *testing.T) {
	is := NewInfoStore()
	is.AddInfo("a", []byte("1"), 0, 1)
	is.AddInfo("b", []byte("2"), 0, 1)

	all := is.AllInfos()
	if len(all) != 2 {
		t.Fatalf("expected 2 infos, got %d", len(all))
	}
}

func TestInfoStoreSequenceMonotonic(t *testing.T) {
	is := NewInfoStore()
	prev := int64(0)
	for range 5 {
		info := is.AddInfo("k", []byte("v"), 0, 1)
		if info.OrigStamp <= prev {
			t.Fatalf("sequence not monotonic: prev=%d, got=%d", prev, info.OrigStamp)
		}
		prev = info.OrigStamp
	}
}

// helper — avoids a nil dereference in test assertions
func (i *Info) GetValue() []byte {
	if i == nil {
		return nil
	}
	return i.Value
}
