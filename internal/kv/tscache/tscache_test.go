package tscache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func ts(wall int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: wall}
}

func TestAdd_GetMax_SingleKey(t *testing.T) {
	t.Parallel()

	c := New(10)
	c.Add([]byte("k"), ts(100))

	if got := c.GetMax([]byte("k")); got != ts(100) {
		t.Fatalf("GetMax = %v, want %v", got, ts(100))
	}
}

func TestAdd_KeepsHighestTimestamp(t *testing.T) {
	t.Parallel()

	c := New(10)
	c.Add([]byte("k"), ts(100))
	c.Add([]byte("k"), ts(50)) // older — should not overwrite
	c.Add([]byte("k"), ts(200)) // newer — should overwrite

	if got := c.GetMax([]byte("k")); got != ts(200) {
		t.Fatalf("GetMax = %v, want %v", got, ts(200))
	}
}

func TestGetMax_UnknownKey_ReturnsLowWater(t *testing.T) {
	t.Parallel()

	c := New(10)
	if got := c.GetMax([]byte("unknown")); !got.IsEmpty() {
		t.Fatalf("GetMax of unknown = %v, want zero (low water)", got)
	}

	c.SetLowWater(ts(500))
	if got := c.GetMax([]byte("unknown")); got != ts(500) {
		t.Fatalf("GetMax of unknown = %v, want %v", got, ts(500))
	}
}

func TestAdd_BelowLowWater_IsNoOp(t *testing.T) {
	t.Parallel()

	c := New(10)
	c.SetLowWater(ts(100))
	c.Add([]byte("k"), ts(50)) // below floor — no entry created

	if c.Len() != 0 {
		t.Fatalf("Len = %d, want 0 (entry below low water should be dropped)", c.Len())
	}
	// GetMax still returns the floor.
	if got := c.GetMax([]byte("k")); got != ts(100) {
		t.Fatalf("GetMax = %v, want low-water %v", got, ts(100))
	}
}

func TestAdd_EqualToLowWater_IsNoOp(t *testing.T) {
	t.Parallel()

	c := New(10)
	c.SetLowWater(ts(100))
	c.Add([]byte("k"), ts(100)) // already covered by floor

	if c.Len() != 0 {
		t.Fatalf("Len = %d, want 0", c.Len())
	}
}

func TestEviction_AdvancesLowWater(t *testing.T) {
	t.Parallel()

	c := New(3)
	c.Add([]byte("a"), ts(10))
	c.Add([]byte("b"), ts(20))
	c.Add([]byte("c"), ts(30))

	// Inserting a fourth entry evicts the oldest ("a", ts=10) and sets the
	// low-water mark to 10.
	c.Add([]byte("d"), ts(40))

	if c.Len() != 3 {
		t.Fatalf("Len after eviction = %d, want 3", c.Len())
	}
	if lw := c.LowWater(); lw != ts(10) {
		t.Fatalf("LowWater = %v, want %v", lw, ts(10))
	}
	// "a" is gone, but GetMax(a) now returns the low-water mark.
	if got := c.GetMax([]byte("a")); got != ts(10) {
		t.Fatalf("GetMax(a) after eviction = %v, want low-water %v", got, ts(10))
	}
}

func TestEviction_Repeated(t *testing.T) {
	t.Parallel()

	c := New(2)
	c.Add([]byte("a"), ts(10))
	c.Add([]byte("b"), ts(20))
	c.Add([]byte("c"), ts(30)) // evicts "a", lowWater=10
	c.Add([]byte("d"), ts(40)) // evicts "b", lowWater=20

	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}
	if lw := c.LowWater(); lw != ts(20) {
		t.Fatalf("LowWater = %v, want %v", lw, ts(20))
	}
}

func TestSetLowWater_PrunesEntries(t *testing.T) {
	t.Parallel()

	c := New(10)
	c.Add([]byte("a"), ts(10))
	c.Add([]byte("b"), ts(20))
	c.Add([]byte("c"), ts(30))

	c.SetLowWater(ts(15)) // prunes "a"

	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}
	if got := c.GetMax([]byte("a")); got != ts(15) {
		t.Fatalf("GetMax(a) = %v, want low-water %v", got, ts(15))
	}
	if got := c.GetMax([]byte("b")); got != ts(20) {
		t.Fatalf("GetMax(b) = %v, want %v (still above floor)", got, ts(20))
	}
}

func TestSetLowWater_DoesNotRegress(t *testing.T) {
	t.Parallel()

	c := New(10)
	c.SetLowWater(ts(100))
	c.SetLowWater(ts(50)) // attempted regression — ignored

	if lw := c.LowWater(); lw != ts(100) {
		t.Fatalf("LowWater = %v, want %v (no regression)", lw, ts(100))
	}
}

func TestUnboundedCache(t *testing.T) {
	t.Parallel()

	// maxSize=0 disables capacity-based eviction.
	c := New(0)
	for i := range 1000 {
		c.Add(fmt.Appendf(nil, "k%d", i), ts(int64(i+1)))
	}
	if c.Len() != 1000 {
		t.Fatalf("Len = %d, want 1000", c.Len())
	}
	if !c.LowWater().IsEmpty() {
		t.Fatalf("LowWater = %v, want zero (no eviction)", c.LowWater())
	}
}

func TestConcurrent_NoRace(t *testing.T) {
	t.Parallel()

	c := New(100)
	var wg sync.WaitGroup
	const goroutines = 20
	const perGoroutine = 500

	wg.Add(goroutines)
	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				key := fmt.Appendf(nil, "g%d-k%d", g, i)
				c.Add(key, ts(int64(g*perGoroutine+i+1)))
				_ = c.GetMax(key)
			}
		}(g)
	}
	wg.Wait()

	// No assertion on final state — the test is checking that the race
	// detector finds no data races.
}

// A practical scenario: reader reads at ts=10, writer tries to write at ts=5.
// The writer must be pushed to at least GetMax(key).Next() = ts(10).Next().
func TestScenario_WriterPushedPastReader(t *testing.T) {
	t.Parallel()

	c := New(10)
	c.Add([]byte("account:42"), ts(10))

	writeTS := ts(5)
	cached := c.GetMax([]byte("account:42"))
	if !writeTS.LessEq(cached) {
		t.Fatalf("scenario broken: writeTS %v should be <= cached %v", writeTS, cached)
	}

	// The coordinator would now push writeTS to cached.Next().
	pushed := cached.Next()
	if !cached.Less(pushed) {
		t.Fatalf("pushed %v should be > cached %v", pushed, cached)
	}
}
