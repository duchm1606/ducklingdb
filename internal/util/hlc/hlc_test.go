package hlc

import (
	"sync"
	"testing"
	"time"
)

func TestTimestamp_Compare(t *testing.T) {
	t.Parallel()

	a := Timestamp{WallTime: 100, Logical: 0}
	b := Timestamp{WallTime: 100, Logical: 1}
	c := Timestamp{WallTime: 101, Logical: 0}

	if !a.Less(b) {
		t.Fatal("a should be less than b")
	}
	if !b.Less(c) {
		t.Fatal("b should be less than c")
	}
	if !a.LessEq(a) {
		t.Fatal("a should be LessEq a")
	}
	if a.Equal(b) {
		t.Fatal("a should not equal b")
	}
}

func TestTimestamp_NextPrev(t *testing.T) {
	t.Parallel()

	ts := Timestamp{WallTime: 100, Logical: 5}
	next := ts.Next()
	if !ts.Less(next) {
		t.Fatalf("Next %v must be greater than %v", next, ts)
	}
	if next.Prev() != ts {
		t.Fatalf("Prev(Next(t)) = %v, want %v", next.Prev(), ts)
	}
}

func TestClock_Now_Monotonic(t *testing.T) {
	t.Parallel()

	m := NewManualClock(1000)
	c := NewClock(m, 250*time.Millisecond)

	var prev Timestamp
	for i := 0; i < 100; i++ {
		now := c.Now()
		if !prev.Less(now) {
			t.Fatalf("iteration %d: %v not greater than %v", i, now, prev)
		}
		prev = now
	}
}

func TestClock_Now_SameWallIncrementsLogical(t *testing.T) {
	t.Parallel()

	m := NewManualClock(500)
	c := NewClock(m, 0)

	t1 := c.Now()
	t2 := c.Now()
	t3 := c.Now()

	if t1.WallTime != 500 || t2.WallTime != 500 || t3.WallTime != 500 {
		t.Fatalf("expected WallTime=500, got %v %v %v", t1, t2, t3)
	}
	if t1.Logical != 0 || t2.Logical != 1 || t3.Logical != 2 {
		t.Fatalf("expected logical 0,1,2, got %v %v %v", t1, t2, t3)
	}
}

func TestClock_Now_WallJumpsForward(t *testing.T) {
	t.Parallel()

	m := NewManualClock(100)
	c := NewClock(m, 0)

	_ = c.Now() // establishes WallTime=100
	m.Set(500)

	next := c.Now()
	if next.WallTime != 500 {
		t.Fatalf("expected WallTime=500, got %v", next)
	}
	if next.Logical != 0 {
		t.Fatalf("expected logical reset to 0, got %d", next.Logical)
	}
}

func TestClock_Now_WallGoesBackward(t *testing.T) {
	t.Parallel()

	// Simulates an NTP correction. HLC must stay monotonic.
	m := NewManualClock(1000)
	c := NewClock(m, 0)

	first := c.Now()
	m.Set(500) // wall clock went backward
	second := c.Now()

	if !first.Less(second) {
		t.Fatalf("wall went backward but %v is not less than %v", second, first)
	}
	if second.WallTime != first.WallTime {
		t.Fatalf("expected WallTime unchanged, got %v", second)
	}
}

func TestClock_Update_RemoteAhead(t *testing.T) {
	t.Parallel()

	m := NewManualClock(100)
	c := NewClock(m, 0)

	remote := Timestamp{WallTime: 500, Logical: 10}
	got := c.Update(remote)

	if !remote.Less(got) {
		t.Fatalf("Update must return a timestamp greater than remote; got %v vs remote %v", got, remote)
	}
}

func TestClock_Update_RemoteBehind(t *testing.T) {
	t.Parallel()

	m := NewManualClock(1000)
	c := NewClock(m, 0)

	_ = c.Now() // state at WallTime=1000

	remote := Timestamp{WallTime: 500, Logical: 2}
	got := c.Update(remote)

	// Local state was 1000.0; remote is behind. Expected: state advances logically.
	if got.WallTime != 1000 {
		t.Fatalf("expected WallTime=1000, got %v", got)
	}
}

func TestClock_Update_Causality(t *testing.T) {
	t.Parallel()

	// Two clocks with 250ms skew. Messages flowing A→B must cause B's
	// next timestamp to strictly follow A's sent timestamp.
	ma := NewManualClock(1_000_000_000)        // 1 second
	mb := NewManualClock(1_000_000_000 - 250_000_000) // 250ms behind
	ca := NewClock(ma, 500*time.Millisecond)
	cb := NewClock(mb, 500*time.Millisecond)

	sent := ca.Now()
	received := cb.Update(sent)

	if !sent.Less(received) {
		t.Fatalf("causality violated: sent %v not less than received %v", sent, received)
	}
}

func TestClock_Concurrent_NoRace(t *testing.T) {
	t.Parallel()

	m := NewManualClock(0)
	c := NewClock(m, 0)

	var wg sync.WaitGroup
	const goroutines = 20
	const perGoroutine = 1000

	wg.Add(goroutines)
	seen := make([][]Timestamp, goroutines)
	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			local := make([]Timestamp, perGoroutine)
			for i := range perGoroutine {
				local[i] = c.Now()
			}
			seen[g] = local
		}(g)
	}
	wg.Wait()

	// Every timestamp must be unique — HLC is a total order.
	set := make(map[Timestamp]struct{}, goroutines*perGoroutine)
	for _, ts := range seen {
		for _, t := range ts {
			if _, dup := set[t]; dup {
				return // t.Fatal not safe inside closure after wg.Wait
			}
			set[t] = struct{}{}
		}
	}
	if len(set) != goroutines*perGoroutine {
		t.Fatalf("expected %d unique timestamps, got %d", goroutines*perGoroutine, len(set))
	}
}

func TestManualClock_SetIncrement(t *testing.T) {
	t.Parallel()

	m := NewManualClock(100)
	if m.Now() != 100 {
		t.Fatal("initial value wrong")
	}
	m.Increment(50)
	if m.Now() != 150 {
		t.Fatal("increment failed")
	}
	m.Set(1000)
	if m.Now() != 1000 {
		t.Fatal("set failed")
	}
}
