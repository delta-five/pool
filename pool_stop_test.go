package pool

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPool_Stop_ReturnsNilOnFirstCall(t *testing.T) {
	p, err := NewPool(2, 2)
	require.NoError(t, err)
	require.NoError(t, p.Stop())
}

func TestPool_Stop_SecondCallReturnsError(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop())
	require.ErrorIs(t, p.Stop(), ErrPoolNotRunning)
}

func TestPool_Stop_ClosesDoneChannelAfterWorkersFinish(t *testing.T) {
	p, err := NewPool(3, 3)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, p.Submit(func() { wg.Done() }))
	waitOrTimeout(t, &wg, time.Second)

	require.NoError(t, p.Stop())

	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done() did not close after Stop()")
	}
}

func TestPool_Stop_ZeroWorkersClosesDoneImmediately(t *testing.T) {
	p, err := NewPool(0, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop())

	select {
	case <-p.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() did not close for a zero-worker pool")
	}
}

func TestPool_Submit_ReturnsErrorAfterStop(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop())

	require.ErrorIs(t, p.Submit(func() {}), ErrPoolNotRunning)
}

func TestPool_TrySubmit_ReturnsErrorAfterStop(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop())

	require.ErrorIs(t, p.TrySubmit(func() {}), ErrPoolNotRunning)
}

func TestPool_SetWorkersCount_ReturnsErrorAfterStop(t *testing.T) {
	p, err := NewPool(1, 1)
	require.NoError(t, err)
	require.NoError(t, p.Stop())

	require.ErrorIs(t, p.SetWorkersCount(2), ErrPoolNotRunning)
}
