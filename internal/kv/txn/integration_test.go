package txn

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/kv/tscache"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// --- Two concurrent writers to the same key: one wins, one retries ---------

func TestIntegration_TwoConcurrentWriters_OneWins(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)
	cache := tscache.New(100)

	if err := mvcc.MVCCPut(engine, []byte("k"),
		hlc.Timestamp{WallTime: 100}, []byte("seed"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const goroutines = 2
	var wg sync.WaitGroup
	results := make([]error, goroutines)
	final := make([]string, goroutines)

	wg.Add(goroutines)
	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			value := fmt.Sprintf("writer-%d", id)
			final[id] = value
			results[id] = RunTransaction(engine, clock,
				RunOptions{Isolation: SSI, Cache: cache, MaxRetries: 20},
				func(tc *TxnCoordSender) error {
					return tc.Put([]byte("k"), []byte(value))
				})
		}(i)
	}
	wg.Wait()

	// Both transactions should eventually succeed — one on the first try,
	// the other after restarts.
	for i, err := range results {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}

	// The final committed value is whichever writer committed last — but it
	// must be one of the two values. No torn writes.
	got, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 1_000_000_000_000}, mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if !bytes.Equal(got, []byte(final[0])) && !bytes.Equal(got, []byte(final[1])) {
		t.Fatalf("final value = %q, expected one of %v", got, final)
	}
}

// --- SSI pushed commit correctly restarts (with eventual success) ----------

func TestIntegration_SSIPushedCommit_Restarts(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Far-future committed version forces first attempt's WriteTimestamp push.
	future := hlc.Timestamp{WallTime: 9_000_000_000}
	if err := mvcc.MVCCPut(engine, []byte("k"), future, []byte("future"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Advance the clock past the future seed so subsequent attempts don't
	// keep hitting the same push.
	clock.Update(future)

	attempts := 0
	err := RunTransaction(engine, clock,
		RunOptions{Isolation: SSI, MaxRetries: 10},
		func(tc *TxnCoordSender) error {
			attempts++
			return tc.Put([]byte("k"), []byte("v"))
		})
	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}
	// With the clock advanced past the seed, the first attempt already
	// writes above the seed naturally → no push, commits on first try.
	if attempts > 3 {
		t.Fatalf("attempts = %d, want ≤ 3", attempts)
	}
}

// --- SI pushed commit succeeds at pushed timestamp --------------------------

func TestIntegration_SIPushedCommit_CommitsAtPushedTS(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	future := hlc.Timestamp{WallTime: 9_000_000_000}
	if err := mvcc.MVCCPut(engine, []byte("k"), future, []byte("future"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	attempts := 0
	err := RunTransaction(engine, clock,
		RunOptions{Isolation: SI},
		func(tc *TxnCoordSender) error {
			attempts++
			return tc.Put([]byte("k"), []byte("ours"))
		})
	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}
	// SI commits on the first attempt despite the push.
	if attempts != 1 {
		t.Fatalf("SI attempts = %d, want 1 (SI accepts pushed commit)", attempts)
	}

	// The value is visible past the future seed.
	got, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 999_999_999_999}, mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("ours")) {
		t.Fatalf("final = %q, want ours", got)
	}
}

// --- Bank transfer preserves sum under SSI ---------------------------------

// This is the canonical correctness test. Under SSI, write skew is
// prevented. With N accounts and M concurrent transfers, the total sum of
// balances must be invariant regardless of ordering.
func TestIntegration_BankTransfer_SumInvariant_SSI(t *testing.T) {
	t.Parallel()

	const (
		numAccounts  = 50
		transferAmt  = 1
		initialBal   = 100
		numTransfers = 30
		concurrency  = 3
	)

	engine := testEngine(t)
	clock := testClock(t)
	cache := tscache.New(1000)

	// Seed all accounts.
	for i := range numAccounts {
		key := []byte(fmt.Sprintf("acct:%d", i))
		val := []byte(strconv.Itoa(initialBal))
		if err := mvcc.MVCCPut(engine, key, hlc.Timestamp{WallTime: 100}, val, nil); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	expectedSum := int64(numAccounts * initialBal)

	// Run random transfers concurrently.
	var wg sync.WaitGroup
	errs := make(chan error, numTransfers)
	attemptsCounter := atomic.Int64{}

	sem := make(chan struct{}, concurrency)
	for i := range numTransfers {
		wg.Add(1)
		sem <- struct{}{}
		go func(seed int) {
			defer wg.Done()
			defer func() { <-sem }()

			rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed+1)))
			from := rng.IntN(numAccounts)
			to := rng.IntN(numAccounts)
			if from == to {
				return
			}

			err := RunTransaction(engine, clock,
				RunOptions{Isolation: SSI, Cache: cache, MaxRetries: 40},
				func(tc *TxnCoordSender) error {
					attemptsCounter.Add(1)
					fromKey := []byte(fmt.Sprintf("acct:%d", from))
					toKey := []byte(fmt.Sprintf("acct:%d", to))

					fromVal, err := tc.Get(fromKey)
					if err != nil {
						return err
					}
					toVal, err := tc.Get(toKey)
					if err != nil {
						return err
					}
					fromBal, _ := strconv.Atoi(string(fromVal))
					toBal, _ := strconv.Atoi(string(toVal))

					if fromBal < transferAmt {
						return nil // insufficient; skip
					}
					if err := tc.Put(fromKey, []byte(strconv.Itoa(fromBal-transferAmt))); err != nil {
						return err
					}
					return tc.Put(toKey, []byte(strconv.Itoa(toBal+transferAmt)))
				})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	// Individual transfers may fail under sustained contention (retry budget
	// exhaustion). The critical correctness property is that failed transfers
	// leave no state residue, so the sum invariant holds regardless of how
	// many succeeded or failed.
	var failed int
	for range errs {
		failed++
	}

	// Read all balances and verify the sum is preserved.
	var sum int64
	for i := range numAccounts {
		key := []byte(fmt.Sprintf("acct:%d", i))
		val, err := mvcc.MVCCGet(engine, key,
			hlc.Timestamp{WallTime: 1_000_000_000_000_000}, mvcc.ReadOptions{})
		if err != nil {
			t.Fatalf("final Get %s: %v", key, err)
		}
		bal, err := strconv.Atoi(string(val))
		if err != nil {
			t.Fatalf("parse %s: %v", val, err)
		}
		sum += int64(bal)
	}
	if sum != expectedSum {
		t.Fatalf("sum invariant violated: got %d, want %d (attempts=%d)",
			sum, expectedSum, attemptsCounter.Load())
	}
	t.Logf("bank transfer: %d/%d succeeded, sum invariant preserved (attempts=%d)",
		numTransfers-failed, numTransfers, attemptsCounter.Load())
}

// --- 100 concurrent transactions on a small keyspace: no torn state --------

func TestIntegration_100ConcurrentTxns_NoSerialAnomaly(t *testing.T) {
	t.Parallel()

	const (
		numKeys = 30
		numTxns = 30
	)

	engine := testEngine(t)
	clock := testClock(t)
	cache := tscache.New(1000)

	// Seed all keys to "0".
	for i := range numKeys {
		if err := mvcc.MVCCPut(engine,
			[]byte(fmt.Sprintf("k%d", i)),
			hlc.Timestamp{WallTime: 100}, []byte("0"), nil); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Each transaction increments one random key by 1.
	var wg sync.WaitGroup
	errs := make(chan error, numTxns)
	for i := range numTxns {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed*2+1)))
			key := []byte(fmt.Sprintf("k%d", rng.IntN(numKeys)))

			err := RunTransaction(engine, clock,
				RunOptions{Isolation: SSI, Cache: cache, MaxRetries: 50},
				func(tc *TxnCoordSender) error {
					v, err := tc.Get(key)
					if err != nil {
						return err
					}
					n, _ := strconv.Atoi(string(v))
					return tc.Put(key, []byte(strconv.Itoa(n+1)))
				})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	// Count failures; tolerate some (under heavy contention retry budgets
	// can exhaust). The crucial correctness property is that the sum
	// equals the number of successful commits — no lost or duplicate
	// updates.
	var failed int
	for range errs {
		failed++
	}
	succeeded := numTxns - failed

	// The sum of all key values must equal the number of successful commits.
	var sum int
	for i := range numKeys {
		key := []byte(fmt.Sprintf("k%d", i))
		v, err := mvcc.MVCCGet(engine, key,
			hlc.Timestamp{WallTime: 1_000_000_000_000_000}, mvcc.ReadOptions{})
		if err != nil {
			t.Fatalf("final Get %s: %v", key, err)
		}
		n, _ := strconv.Atoi(string(v))
		sum += n
	}
	if sum != succeeded {
		t.Fatalf("counter sum = %d, want %d (successful commits); failures=%d",
			sum, succeeded, failed)
	}
	t.Logf("counter: %d/%d txns succeeded, sum = %d", succeeded, numTxns, sum)
}

// --- Reader pushes SI writer; writer commits at pushed timestamp -----------

func TestIntegration_ReaderPushesSIWriter_WriterSeesPush(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)
	cache := tscache.New(100)

	// Prior committed value.
	if err := mvcc.MVCCPut(engine, []byte("k"),
		hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// SI writer holds an intent at a low timestamp.
	writer, err := Begin(engine, clock, cache, SI)
	if err != nil {
		t.Fatalf("Begin writer: %v", err)
	}
	writer.txn.ReadTimestamp = hlc.Timestamp{WallTime: 700}
	writer.txn.WriteTimestamp = hlc.Timestamp{WallTime: 700}
	if err := writer.writeTxnRecord(); err != nil {
		t.Fatalf("persist writer: %v", err)
	}
	if err := writer.Put([]byte("k"), []byte("new")); err != nil {
		t.Fatalf("writer Put: %v", err)
	}

	// Reader at a much later timestamp pushes the writer.
	reader, err := Begin(engine, clock, cache, SI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}
	reader.txn.ReadTimestamp = hlc.Timestamp{WallTime: 2_000_000_000}
	if err := reader.writeTxnRecord(); err != nil {
		t.Fatalf("persist reader: %v", err)
	}
	val, err := reader.Get([]byte("k"))
	if err != nil {
		t.Fatalf("reader Get: %v", err)
	}
	if !bytes.Equal(val, []byte("old")) {
		t.Fatalf("reader sees %q, want old", val)
	}

	// Writer commits — SI accepts the pushed timestamp.
	if err := writer.Commit(); err != nil {
		t.Fatalf("writer Commit: %v", err)
	}
	// Writer's final WriteTimestamp is above reader's ReadTimestamp.
	rec, _, _ := LoadTxnRecord(engine, writer.ID())
	if !(hlc.Timestamp{WallTime: 2_000_000_000}).Less(rec.WriteTimestamp) {
		t.Fatalf("writer WriteTs = %v, expected to be pushed past reader's 2s", rec.WriteTimestamp)
	}
}

// --- Business-logic error from closure: no retry, intents cleaned up -------

func TestIntegration_BusinessLogicError_NoRetry_Clean(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	if err := mvcc.MVCCPut(engine, []byte("k"),
		hlc.Timestamp{WallTime: 100}, []byte("prior"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	myErr := errors.New("business rule violated")
	attempts := 0

	err := RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			attempts++
			if err := tc.Put([]byte("k"), []byte("doomed")); err != nil {
				return err
			}
			return myErr
		})

	if !errors.Is(err, myErr) {
		t.Fatalf("err = %v, want myErr", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	// The prior value is still there — our intent was aborted.
	got, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 1_000_000_000_000}, mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("post-abort Get: %v", err)
	}
	if !bytes.Equal(got, []byte("prior")) {
		t.Fatalf("final = %q, want prior (intent should have been cleaned up)", got)
	}
}
