package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitOrTimeout(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for tasks to complete")
	}
}

func TestNewPool_RejectsNegativeWorkersCount(t *testing.T) {
	p, err := NewPool(-1, 1)
	require.Error(t, err)
	assert.Nil(t, p)
}

func TestNewPool_RejectsNegativeQueueSize(t *testing.T) {
	p, err := NewPool(1, -1)
	require.Error(t, err)
	assert.Nil(t, p)
}

func TestNewPool_AcceptsZeroWorkersAndZeroQueue(t *testing.T) {
	p, err := NewPool(0, 0)
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewPool_AcceptsPositiveValues(t *testing.T) {
	p, err := NewPool(2, 4)
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestPool_Submit_RunsTask(t *testing.T) {
	p, err := NewPool(2, 2)
	require.NoError(t, err)

	done := make(chan struct{})
	require.NoError(t, p.Submit(func() { close(done) }))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task did not run in time")
	}
}

func TestPool_Submit_AllSubmittedTasksRun(t *testing.T) {
	p, err := NewPool(4, 10)
	require.NoError(t, err)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	var count atomic.Int64
	for range n {
		require.NoError(t, p.Submit(func() {
			count.Add(1)
			wg.Done()
		}))
	}

	waitOrTimeout(t, &wg, 2*time.Second)
	assert.Equal(t, int64(n), count.Load())
	assert.Equal(t, int64(n), p.Statistic().TaskProcessed)
}

func TestPool_TrySubmit_SucceedsWhenSpaceAvailable(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)

	done := make(chan struct{})
	require.NoError(t, p.TrySubmit(func() { close(done) }))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task did not run in time")
	}
}

func TestPool_TrySubmit_ReturnsErrQueueFullWhenFull(t *testing.T) {
	// Zero workers: nothing drains the queue, so "full" is deterministic
	// instead of racing against a live worker.
	p, err := NewPool(0, 2)
	require.NoError(t, err)

	require.NoError(t, p.TrySubmit(func() {}))
	require.NoError(t, p.TrySubmit(func() {}))

	err = p.TrySubmit(func() {})
	require.Error(t, err)
	assert.Same(t, errQueueFull, err)
}

func TestPool_Submit_PanicInTaskDoesNotStopWorker(t *testing.T) {
	p, err := NewPool(1, 2)
	require.NoError(t, err)

	require.NoError(t, p.Submit(func() { panic("boom") }))

	done := make(chan struct{})
	require.NoError(t, p.Submit(func() { close(done) }))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not process the next task after recovering from a panic")
	}

	require.Eventually(t, func() bool {
		return p.Statistic().PanicsCount == 1
	}, time.Second, 10*time.Millisecond)
}

func TestPool_Statistic_TracksProcessedTasks(t *testing.T) {
	p, err := NewPool(1, 4)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(3)
	for range 3 {
		require.NoError(t, p.Submit(func() { wg.Done() }))
	}
	waitOrTimeout(t, &wg, time.Second)

	assert.Equal(t, int64(3), p.Statistic().TaskProcessed)
}

func TestPool_Statistic_WorkersActiveReturnsToZeroAfterCompletion(t *testing.T) {
	p, err := NewPool(2, 4)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, p.Submit(func() { wg.Done() }))
	waitOrTimeout(t, &wg, time.Second)

	require.Eventually(t, func() bool {
		return p.Statistic().WorkersActive == 0
	}, time.Second, 10*time.Millisecond)
}

func TestPool_SetWorkersCount_RejectsNegative(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)

	require.Error(t, p.SetWorkersCount(-1))
}

func TestPool_SetWorkersCount_Increase_NewWorkersProcessQueuedTasks(t *testing.T) {
	p, err := NewPool(0, 10) // starts with no workers, nothing drains the queue yet.
	require.NoError(t, err)

	require.NoError(t, p.SetWorkersCount(2))

	var wg sync.WaitGroup
	wg.Add(4)
	for range 4 {
		require.NoError(t, p.Submit(func() { wg.Done() }))
	}
	waitOrTimeout(t, &wg, 2*time.Second)
}

func TestPool_SetWorkersCount_Decrease_LimitsConcurrency(t *testing.T) {
	p, err := NewPool(4, 20)
	require.NoError(t, err)

	require.NoError(t, p.SetWorkersCount(1))
	// Give the shrink goroutine time to hand out all its close signals before
	// we start measuring concurrency.
	time.Sleep(100 * time.Millisecond)

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	var current, maxConcurrent atomic.Int64
	for range n {
		require.NoError(t, p.Submit(func() {
			c := current.Add(1)
			for {
				m := maxConcurrent.Load()
				if c <= m || maxConcurrent.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			current.Add(-1)
			wg.Done()
		}))
	}
	waitOrTimeout(t, &wg, 3*time.Second)

	assert.LessOrEqual(t, maxConcurrent.Load(), int64(1),
		"known bug: SetWorkersCount's shrink goroutine returns after the first successful "+
			"killWorker() call instead of looping until delta reaches 0, so shrinking by more "+
			"than one worker at a time only actually stops one")
}

func TestPool_SetWorkersCount_RepeatedCallsDoNotDrift(t *testing.T) {
	p, err := NewPool(2, 1)
	require.NoError(t, err)

	require.NoError(t, p.SetWorkersCount(4))
	require.NoError(t, p.SetWorkersCount(4))

	p.lock.RLock()
	got := p.workersCount
	p.lock.RUnlock()

	assert.Equal(t, 4, got)
}

func TestPool_Statistic_ReturnsZeroValueForFreshPool(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)

	stat := p.Statistic()
	assert.Equal(t, int64(0), stat.TaskProcessed)
	assert.Equal(t, int64(0), stat.PanicsCount)
}
