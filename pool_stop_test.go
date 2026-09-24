package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool_Stop_ReturnsNilOnFirstCall(t *testing.T) {
	p, err := NewPool(2, 2)
	require.NoError(t, err)
	require.NoError(t, p.Stop(false))
}

func TestPool_Stop_SecondCallReturnsError(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop(false))
	require.ErrorIs(t, p.Stop(false), ErrPoolNotRunning)
}

func TestPool_Stop_ClosesDoneChannelAfterWorkersFinish(t *testing.T) {
	p, err := NewPool(3, 3)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, p.Submit(func() { wg.Done() }))
	waitOrTimeout(t, &wg, time.Second)

	require.NoError(t, p.Stop(false))

	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done() did not close after Stop()")
	}
}

func TestPool_Stop_ZeroWorkersClosesDoneImmediately(t *testing.T) {
	p, err := NewPool(0, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop(false))

	select {
	case <-p.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() did not close for a zero-worker pool")
	}
}

func TestPool_Submit_ReturnsErrorAfterStop(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop(false))

	require.ErrorIs(t, p.Submit(func() {}), ErrPoolNotRunning)
}

func TestPool_TrySubmit_ReturnsErrorAfterStop(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop(false))

	require.ErrorIs(t, p.TrySubmit(func() {}), ErrPoolNotRunning)
}

func TestPool_SetWorkersCount_ReturnsErrorAfterStop(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop(false))

	require.ErrorIs(t, p.SetWorkersCount(2), ErrPoolNotRunning)
}

func TestPool_Stop_DropTasksFalse_DrainsQueuedTasksBeforeExit(t *testing.T) {
	// A single worker so the in-flight task blocks all queued tasks behind it
	// until Stop is called, making the drain-vs-drop race deterministic.
	p, err := NewPool(1, 10)
	require.NoError(t, err)

	block := make(chan struct{})
	require.NoError(t, p.Submit(func() { <-block }))

	const n = 5
	var processed atomic.Int64
	for range n {
		require.NoError(t, p.Submit(func() { processed.Add(1) }))
	}

	require.NoError(t, p.Stop(false))
	close(block)

	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done() did not close after Stop(false)")
	}

	assert.Equal(t, int64(n), processed.Load())
}

func TestPool_Stop_DropTasksTrue_DropsQueuedTasks(t *testing.T) {
	p, err := NewPool(1, 10)
	require.NoError(t, err)

	block := make(chan struct{})
	require.NoError(t, p.Submit(func() { <-block }))

	var processed atomic.Int64
	for range 5 {
		require.NoError(t, p.Submit(func() { processed.Add(1) }))
	}

	require.NoError(t, p.Stop(true))
	close(block)

	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done() did not close after Stop(true)")
	}

	assert.Equal(t, int64(0), processed.Load())
}

func TestPool_Stop_DropTasksTrue_LetsInFlightTaskFinish(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)

	started := make(chan struct{})
	finish := make(chan struct{})
	var ran atomic.Bool
	require.NoError(t, p.Submit(func() {
		close(started)
		<-finish
		ran.Store(true)
	}))

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("task did not start in time")
	}

	require.NoError(t, p.Stop(true))
	close(finish)

	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done() did not close after Stop(true)")
	}

	assert.True(t, ran.Load())
}

func TestPool_Stop_SecondCallIgnoresDropTasksArgument(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop(false))
	require.ErrorIs(t, p.Stop(true), ErrPoolNotRunning)
}
