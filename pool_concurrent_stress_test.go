package pool

import (
	"sync"
	"testing"
)

// These stress tests race Submit/TrySubmit/SetWorkersCount against a
// concurrent Stop() call. They pin two known, unfixed bugs:
//
//   - Submit, TrySubmit, SetWorkersCount and the internal killWorker all call
//     p.submitWG.Add(1) unconditionally *before* checking p.stopCalled, with
//     no synchronization preventing that Add from racing with Stop's
//     p.submitWG.Wait() while the counter is transitioning through zero.
//     That is exactly the misuse sync.WaitGroup's docs warn about ("Note
//     that calls with a positive delta that start when the counter is zero
//     must happen before a Wait"), and it is reproducible both as an
//     explicit "sync: WaitGroup is reused before previous Wait has
//     returned" panic and, in other interleavings, as a genuine data race on
//     p.taskCh / p.workerCloseCh flagged by `go test -race` (Stop() writing
//     `p.taskCh = nil` concurrently with Submit reading p.taskCh to build
//     its select statement).
//
// Every goroutine below recovers its own panics so a hit doesn't crash the
// whole test binary; a recovered panic is still reported via t.Errorf, so
// these tests fail (rather than hang or silently pass) whenever the race
// actually fires. Run with `-race` to also catch the data-race variant.
func TestPool_ConcurrentSubmitAndTrySubmitVsStop(t *testing.T) {
	const iterations = 3000
	for range iterations {
		p, err := NewPool(2, 1)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Submit panicked racing with Stop: %v", r)
				}
			}()
			_ = p.Submit(func() {})
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("TrySubmit panicked racing with Stop: %v", r)
				}
			}()
			_ = p.TrySubmit(func() {})
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Stop panicked racing with Submit/TrySubmit: %v", r)
				}
			}()
			_ = p.Stop()
		}()
		wg.Wait()
	}
}

func TestPool_ConcurrentSetWorkersCountVsStop(t *testing.T) {
	const iterations = 1500
	for range iterations {
		p, err := NewPool(4, 1)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("SetWorkersCount panicked racing with Stop: %v", r)
				}
			}()
			_ = p.SetWorkersCount(1)
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Stop panicked racing with SetWorkersCount: %v", r)
				}
			}()
			_ = p.Stop()
		}()
		wg.Wait()
	}
}
