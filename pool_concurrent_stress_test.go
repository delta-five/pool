package pool

import (
	"sync"
	"testing"
)

// These stress tests race Submit/TrySubmit/SetWorkersCount against a
// concurrent Stop() call. Submit, TrySubmit, SetWorkersCount and the
// internal killWorker all check p.stopCalled and register with
// p.submitWG.Add(1) in the same p.lock critical section that Stop() takes
// (held for the whole of Stop(), including p.submitWG.Wait()), so an
// in-flight call is always fully ordered before or fully ordered after
// Stop() closes p.taskCh.
//
// Every goroutine below recovers its own panics so a hit doesn't crash the
// whole test binary; a recovered panic is still reported via t.Errorf, so
// these tests fail (rather than hang or silently pass) if this synchronization
// ever regresses. Run with `-race` to also catch data-race variants.
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
			_ = p.Stop(false)
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
			_ = p.Stop(false)
		}()
		wg.Wait()
	}
}
