package lsm

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	// immutableCount keys are written once, before any writer starts, and are
	// never touched again. Every iterator must therefore observe all of them.
	immutableCount = 200
	immutablePfx   = "imm-"
)

func immutableKey(i int) []byte { return fmt.Appendf(nil, "%s%06d", immutablePfx, i) }
func immutableVal(i int) []byte { return fmt.Appendf(nil, "immval-%06d", i) }
func churnKey(i int) []byte     { return fmt.Appendf(nil, "key-%06d", i) }

// TestIteratorStableUnderConcurrentWrites is the acceptance test for the
// storage.Iterator observation contract.
//
// Before the contract existed, LSMEngine.NewIterator bound the *live* MemTable
// and the *live* SSTable readers. Writers then mutated the same skiplist
// (MemTable.insertNode racing MemTable.seekNode) and flush/compaction closed
// reader file handles beneath in-flight iterators. Separately,
// SSTableReader.readRecordAt used Seek+ReadFull on a shared *os.File, so two
// iterators over one reader interleaved their seek/read pairs and spliced each
// other's records — a corruption the Go race detector cannot see, because the
// shared state is an OS file offset rather than memory.
//
// The test drives that shape: N writers mutating the engine while M readers hold
// long-lived iterators across flush boundaries. It asserts two independent
// properties, and both have caught real bugs:
//
//  1. Keys from one iterator are strictly increasing. This detected the live
//     memtable race (a cursor jumping backwards) and the shared-fd corruption
//     (a key spliced out of a neighbouring value) — neither of which needed
//     -race to surface.
//  2. Every iterator observes the complete immutable key set with correct
//     values. This closes the hole that property 1 alone left open: a totally
//     broken iterator returning zero rows satisfies "strictly increasing"
//     vacuously, because a read error makes a source merely go Valid()==false
//     and MergeIterator silently skips it.
func TestIteratorStableUnderConcurrentWrites(t *testing.T) {
	// A small threshold forces many flush + compaction cycles during the run,
	// which is what exercises reader pinning.
	engine, err := OpenLSM(LSMOptions{Dir: t.TempDir(), MemTableThreshold: 4 << 10})
	if err != nil {
		t.Fatalf("OpenLSM: %v", err)
	}
	defer engine.Close()

	// Immutable set — disjoint from everything the writers touch.
	for i := range immutableCount {
		if err := engine.Put(immutableKey(i), immutableVal(i)); err != nil {
			t.Fatalf("seed immutable: %v", err)
		}
	}
	// Some churn keys so the writers are not starting from empty.
	for i := range 200 {
		if err := engine.Put(churnKey(i), []byte("seed")); err != nil {
			t.Fatalf("seed churn: %v", err)
		}
	}

	const (
		writers = 4
		readers = 4
	)
	stop := make(chan struct{})
	errCh := make(chan error, writers+readers)
	var wg sync.WaitGroup

	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				// Never touches the imm- range.
				k := churnKey((w*100000 + i) % 2000)
				if i%4 == 3 {
					if err := engine.Delete(k); err != nil {
						errCh <- fmt.Errorf("writer %d delete: %w", w, err)
						return
					}
					continue
				}
				if err := engine.Put(k, fmt.Appendf(nil, "w%d-%08d", w, i)); err != nil {
					errCh <- fmt.Errorf("writer %d put: %w", w, err)
					return
				}
			}
		}(w)
	}

	for r := range readers {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := scanOnce(engine); err != nil {
					errCh <- fmt.Errorf("reader %d: %w", r, err)
					return
				}
			}
		}(r)
	}

	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}
}

// scanOnce walks the whole engine through one long-lived iterator and checks
// both invariants.
func scanOnce(engine *LSMEngine) error {
	it, err := engine.NewIterator()
	if err != nil {
		return fmt.Errorf("NewIterator: %w", err)
	}
	defer it.Close()

	var prev []byte
	seenImmutable := make(map[string][]byte, immutableCount)

	for ok := it.Seek(nil); ok; ok = it.Next() {
		k := it.Key()
		if prev != nil && bytes.Compare(k, prev) <= 0 {
			return fmt.Errorf("keys not strictly increasing: %q after %q", k, prev)
		}
		prev = append(prev[:0:0], k...)

		if bytes.HasPrefix(k, []byte(immutablePfx)) && !it.IsTombstone() {
			seenImmutable[string(k)] = append([]byte(nil), it.Value()...)
		}
	}

	// Completeness: the immutable set is written before any writer starts and
	// never modified, so a point-in-time view must contain all of it. A
	// truncated scan — a closed file handle, a dropped SSTable — shows up here
	// and nowhere else.
	if len(seenImmutable) != immutableCount {
		return fmt.Errorf("scan returned %d/%d immutable keys; iterator lost data",
			len(seenImmutable), immutableCount)
	}
	for i := range immutableCount {
		k, want := string(immutableKey(i)), immutableVal(i)
		got, ok := seenImmutable[k]
		if !ok {
			return fmt.Errorf("immutable key %q missing from scan", k)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("immutable key %q: got %q want %q", k, got, want)
		}
	}
	return nil
}

// TestUnrefUnderflowPanics pins the refcount floor.
//
// Unref had no floor, so an extra release drove refs negative and closed the
// file handle again. That mattered because the design's invariant is "the
// engine holds exactly one reference": once the count can go negative, a later
// double-release on a reader that still has a live iterator pin would close the
// fd underneath that iterator.
func TestUnrefUnderflowPanics(t *testing.T) {
	engine, err := OpenLSM(LSMOptions{Dir: t.TempDir(), MemTableThreshold: 1 << 10})
	if err != nil {
		t.Fatalf("OpenLSM: %v", err)
	}
	defer engine.Close()

	// Force at least one SSTable to exist.
	for i := range 400 {
		if err := engine.Put(churnKey(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	engine.mu.RLock()
	var path string
	for _, level := range engine.levels {
		for _, r := range level {
			path = r.path
			break
		}
		if path != "" {
			break
		}
	}
	engine.mu.RUnlock()
	if path == "" {
		// Not a reason to skip: if no SSTable exists the test's premise is
		// broken and someone should fix it. A skip here would silently retire
		// the only guard on the refcount floor.
		t.Fatal("expected an SSTable to exist; test precondition broken")
	}

	// Underflow an independent reader over the same file. Corrupting the
	// engine's own reader would make engine.Close() panic outside the recover
	// below, failing the test for the wrong reason.
	independent, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("releasing more references than were held should panic")
		}
	}()
	_ = independent.Unref() // sole reference → 0, closes the handle
	_ = independent.Unref() // underflow → must panic
}
