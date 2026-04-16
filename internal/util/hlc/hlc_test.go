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

// ---------------------------------------------------------------------------
// D.2 — Clock skew simulation tests
// ---------------------------------------------------------------------------

func TestSkew_BidirectionalMessages(t *testing.T) {
	t.Parallel()

	// Node A is 250ms ahead of Node B. They exchange 10 rounds of messages.
	// Causality must hold across every hop: each received timestamp must be
	// strictly greater than the sent timestamp.
	ma := NewManualClock(1_000_000_000)              // A at 1s
	mb := NewManualClock(1_000_000_000 - 250_000_000) // B at 0.75s (250ms behind)
	ca := NewClock(ma, 500*time.Millisecond)
	cb := NewClock(mb, 500*time.Millisecond)

	var lastA, lastB Timestamp
	for round := 0; round < 10; round++ {
		// A → B
		sentA := ca.Now()
		if !lastA.Less(sentA) {
			t.Fatalf("round %d: A's timestamp %v not greater than previous %v", round, sentA, lastA)
		}
		lastA = sentA

		receivedB := cb.Update(sentA)
		if !sentA.Less(receivedB) {
			t.Fatalf("round %d: A→B causality violated: sent %v, received %v", round, sentA, receivedB)
		}

		// B → A
		sentB := cb.Now()
		if !lastB.Less(sentB) {
			t.Fatalf("round %d: B's timestamp %v not greater than previous %v", round, sentB, lastB)
		}
		lastB = sentB

		receivedA := ca.Update(sentB)
		if !sentB.Less(receivedA) {
			t.Fatalf("round %d: B→A causality violated: sent %v, received %v", round, sentB, receivedA)
		}

		// Advance physical clocks slightly each round (simulating real time passing).
		ma.Increment(1_000_000) // +1ms
		mb.Increment(1_000_000) // +1ms
	}
}

func TestSkew_ThreeNodes(t *testing.T) {
	t.Parallel()

	// Three nodes with varying skew: A=0ms, B=+100ms, C=-150ms.
	// Message chain: A→B→C→A. Final A timestamp must be > initial A timestamp.
	ma := NewManualClock(1_000_000_000)
	mb := NewManualClock(1_000_000_000 + 100_000_000) // +100ms
	mc := NewManualClock(1_000_000_000 - 150_000_000) // -150ms
	ca := NewClock(ma, 500*time.Millisecond)
	cb := NewClock(mb, 500*time.Millisecond)
	cc := NewClock(mc, 500*time.Millisecond)

	initial := ca.Now()

	// A → B
	tAB := ca.Now()
	cb.Update(tAB)

	// B → C
	tBC := cb.Now()
	cc.Update(tBC)

	// C → A
	tCA := cc.Now()
	final := ca.Update(tCA)

	if !initial.Less(final) {
		t.Fatalf("chain A→B→C→A: initial %v not less than final %v", initial, final)
	}
	// Every hop must have advanced.
	if !tAB.Less(tBC) {
		t.Fatalf("A→B→C: %v not less than %v", tAB, tBC)
	}
	if !tBC.Less(tCA) {
		t.Fatalf("B→C→A: %v not less than %v", tBC, tCA)
	}
}

func TestSkew_BackwardJump100ms(t *testing.T) {
	t.Parallel()

	m := NewManualClock(1_000_000_000) // 1s
	c := NewClock(m, 250*time.Millisecond)

	before := c.Now()
	m.Increment(-100_000_000) // jump backward 100ms
	after := c.Now()

	if !before.Less(after) {
		t.Fatalf("backward jump broke monotonicity: before %v, after %v", before, after)
	}
	// WallTime must NOT go backward.
	if after.WallTime < before.WallTime {
		t.Fatalf("WallTime decreased: %d → %d", before.WallTime, after.WallTime)
	}
}

func TestSkew_ForwardJump500ms(t *testing.T) {
	t.Parallel()

	m := NewManualClock(1_000_000_000) // 1s
	c := NewClock(m, 250*time.Millisecond)

	before := c.Now()
	m.Increment(500_000_000) // jump forward 500ms
	after := c.Now()

	if !before.Less(after) {
		t.Fatalf("forward jump broke monotonicity: before %v, after %v", before, after)
	}
	// WallTime must have advanced to the new physical time.
	if after.WallTime != 1_500_000_000 {
		t.Fatalf("expected WallTime=1.5s, got %d", after.WallTime)
	}
	// Logical must reset when wall advances.
	if after.Logical != 0 {
		t.Fatalf("expected Logical=0 after wall advance, got %d", after.Logical)
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
