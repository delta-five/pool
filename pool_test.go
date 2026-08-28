package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewPool_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		workers int
		queue   int
		wantErr error
	}{
		{"нулевое число воркеров", 0, 1, ErrWrongWorkersCount},
		{"отрицательное число воркеров", -1, 1, ErrWrongWorkersCount},
		{"нулевой размер очереди", 1, 0, ErrWrongTaskQueueSize},
		{"отрицательный размер очереди", 1, -1, ErrWrongTaskQueueSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := NewPool(tt.workers, tt.queue)

			require.Nil(t, p)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestNewPool_Success(t *testing.T) {
	t.Parallel()

	p, err := NewPool(4, 10)

	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, 4, p.Workers())
	require.True(t, p.IsActive())
	require.Equal(t, 0, p.DoneTasks())
	require.Equal(t, 0, p.ExecutingTasks())

	require.NoError(t, p.Shutdown(context.Background(), true))
}

func TestPool_Workers(t *testing.T) {
	t.Parallel()

	p, err := NewPool(2, 10)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background(), true)) })

	require.Equal(t, 2, p.Workers())
}

func TestPool_SetWorkers(t *testing.T) {
	t.Parallel()

	t.Run("увеличение и уменьшение", func(t *testing.T) {
		t.Parallel()

		p, err := NewPool(2, 10)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background(), true)) })

		require.NoError(t, p.SetWorkers(5))
		require.Equal(t, 5, p.Workers())

		require.NoError(t, p.SetWorkers(1))
		require.Equal(t, 1, p.Workers())
	})

	t.Run("некорректное количество", func(t *testing.T) {
		t.Parallel()

		p, err := NewPool(2, 10)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background(), true)) })

		require.ErrorIs(t, p.SetWorkers(0), ErrWrongWorkersCount)
		require.ErrorIs(t, p.SetWorkers(-1), ErrWrongWorkersCount)
	})

	t.Run("после остановки пула", func(t *testing.T) {
		t.Parallel()

		p, err := NewPool(2, 10)
		require.NoError(t, err)
		require.NoError(t, p.Shutdown(context.Background(), true))

		require.ErrorIs(t, p.SetWorkers(3), ErrPoolIsNotActive)
	})
}

func TestPool_ExecutingTasks(t *testing.T) {
	t.Parallel()

	p, err := NewPool(1, 10)
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	require.NoError(t, p.Submit(MakeTask(1, func() { close(started); <-release })))
	<-started

	require.Equal(t, 1, p.ExecutingTasks())
	require.Equal(t, 0, p.DoneTasks())

	close(release)
	require.NoError(t, p.Shutdown(context.Background(), false))

	require.Equal(t, 0, p.ExecutingTasks())
	require.Equal(t, 1, p.DoneTasks())
}

func TestPool_DoneTasks(t *testing.T) {
	t.Parallel()

	p, err := NewPool(2, 10)
	require.NoError(t, err)

	for i := range 5 {
		require.NoError(t, p.Submit(MakeTask(i, func() {})))
	}

	require.NoError(t, p.Shutdown(context.Background(), false))
	require.Equal(t, 5, p.DoneTasks())
}

func TestPool_IsActive(t *testing.T) {
	t.Parallel()

	p, err := NewPool(1, 10)
	require.NoError(t, err)
	require.True(t, p.IsActive())

	require.NoError(t, p.Shutdown(context.Background(), true))
	require.False(t, p.IsActive())
}

func TestPool_Submit_ExecutesTasks(t *testing.T) {
	t.Parallel()

	p, err := NewPool(2, 10)
	require.NoError(t, err)

	const total = 5
	var mu sync.Mutex
	executed := make([]int, 0, total)

	for i := range total {
		n := i
		require.NoError(t, p.Submit(MakeTask(n, func() {
			mu.Lock()
			executed = append(executed, n)
			mu.Unlock()
		})))
	}

	require.NoError(t, p.Shutdown(context.Background(), false))

	require.Len(t, executed, total)
	require.Equal(t, total, p.DoneTasks())
	require.Equal(t, 0, p.ExecutingTasks())
}

func TestPool_Submit_NotActive(t *testing.T) {
	t.Parallel()

	p := &Pool{
		tasksQueue:    make(chan PoolTask, 1),
		submitBreaker: make(chan struct{}),
	}
	p.notActive.Store(true)

	require.ErrorIs(t, p.Submit(MakeTask(1, func() {})), ErrPoolIsNotActive)
}

func TestPool_Submit_BreakerClosed(t *testing.T) {
	t.Parallel()

	p := &Pool{
		tasksQueue:    make(chan PoolTask, 1),
		submitBreaker: make(chan struct{}),
	}
	p.tasksQueue <- MakeTask(1, func() {})
	close(p.submitBreaker)

	require.ErrorIs(t, p.Submit(MakeTask(2, func() {})), ErrPoolIsNotActive)
}

func TestPool_TrySubmit(t *testing.T) {
	t.Parallel()

	t.Run("успешная отправка", func(t *testing.T) {
		t.Parallel()

		p, err := NewPool(1, 1)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background(), true)) })

		require.NoError(t, p.TrySubmit(MakeTask(1, func() {})))
	})

	t.Run("очередь заполнена", func(t *testing.T) {
		t.Parallel()

		p := &Pool{
			tasksQueue:    make(chan PoolTask, 1),
			submitBreaker: make(chan struct{}),
		}
		p.tasksQueue <- MakeTask(1, func() {})

		require.ErrorIs(t, p.TrySubmit(MakeTask(2, func() {})), ErrQueueFull)
	})

	t.Run("пул неактивен", func(t *testing.T) {
		t.Parallel()

		p := &Pool{
			tasksQueue:    make(chan PoolTask, 1),
			submitBreaker: make(chan struct{}),
		}
		p.notActive.Store(true)

		require.ErrorIs(t, p.TrySubmit(MakeTask(1, func() {})), ErrPoolIsNotActive)
	})

	t.Run("breaker закрыт при заполненной очереди", func(t *testing.T) {
		t.Parallel()

		p := &Pool{
			tasksQueue:    make(chan PoolTask, 1),
			submitBreaker: make(chan struct{}),
		}
		p.tasksQueue <- MakeTask(1, func() {})
		close(p.submitBreaker)

		require.ErrorIs(t, p.TrySubmit(MakeTask(2, func() {})), ErrPoolIsNotActive)
	})
}

func TestPool_Submit_WithPanicHandler(t *testing.T) {
	t.Parallel()

	p, err := NewPool(1, 10)
	require.NoError(t, err)

	var mu sync.Mutex
	var ids []any
	var recovered []any

	require.NoError(t, p.Submit(
		MakeTask(42, func() { panic("boom") }),
		WithPanicHandler(func(id any, r any) {
			mu.Lock()
			ids = append(ids, id)
			recovered = append(recovered, r)
			mu.Unlock()
		}),
	))

	require.NoError(t, p.Shutdown(context.Background(), false))

	require.Equal(t, []any{42}, ids)
	require.Equal(t, []any{"boom"}, recovered)
	require.Equal(t, 1, p.DoneTasks())
}

func TestPool_Shutdown_Graceful(t *testing.T) {
	t.Parallel()

	p, err := NewPool(2, 10)
	require.NoError(t, err)

	for i := range 5 {
		require.NoError(t, p.Submit(MakeTask(i, func() {})))
	}

	require.NoError(t, p.Shutdown(context.Background(), false))
	require.Equal(t, 5, p.DoneTasks())
	require.False(t, p.IsActive())
}

func TestPool_Shutdown_Force(t *testing.T) {
	t.Parallel()

	p, err := NewPool(3, 10)
	require.NoError(t, err)

	require.NoError(t, p.Submit(MakeTask(1, func() {})))

	require.NoError(t, p.Shutdown(context.Background(), true))
	require.False(t, p.IsActive())
}

func TestPool_Shutdown_ForceDrainsQueue(t *testing.T) {
	t.Parallel()

	p := &Pool{
		tasksQueue:    make(chan PoolTask, 3),
		submitBreaker: make(chan struct{}),
		workersDone:   make(chan struct{}),
	}
	p.tasksQueue <- MakeTask(1, func() {})
	p.tasksQueue <- MakeTask(2, func() {})
	p.tasksQueue <- MakeTask(3, func() {})

	require.NoError(t, p.Shutdown(context.Background(), true))

	require.Empty(t, p.tasksQueue)
	select {
	case <-p.workersDone:
	default:
		t.Fatal("workersDone должен быть закрыт после Shutdown")
	}
}

func TestPool_Shutdown_AlreadyShutdown(t *testing.T) {
	t.Parallel()

	p, err := NewPool(1, 10)
	require.NoError(t, err)

	require.NoError(t, p.Shutdown(context.Background(), false))
	require.ErrorIs(t, p.Shutdown(context.Background(), false), ErrAlreadyShutdown)
}

func TestPool_Shutdown_ContextCanceled(t *testing.T) {
	t.Parallel()

	t.Run("graceful", func(t *testing.T) {
		t.Parallel()

		p := &Pool{
			tasksQueue:    make(chan PoolTask),
			submitBreaker: make(chan struct{}),
			workersDone:   make(chan struct{}),
		}
		p.workersWG.Add(1)
		t.Cleanup(p.workersWG.Done)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.ErrorIs(t, p.Shutdown(ctx, false), context.Canceled)
	})

	t.Run("force", func(t *testing.T) {
		t.Parallel()

		p := &Pool{
			tasksQueue:    make(chan PoolTask),
			submitBreaker: make(chan struct{}),
			workersDone:   make(chan struct{}),
		}
		p.submitWG.Add(1)
		t.Cleanup(p.submitWG.Done)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.ErrorIs(t, p.Shutdown(ctx, true), context.Canceled)
	})
}

func TestPool_Submit_AfterShutdown(t *testing.T) {
	t.Parallel()

	p, err := NewPool(1, 10)
	require.NoError(t, err)
	require.NoError(t, p.Shutdown(context.Background(), false))

	require.ErrorIs(t, p.Submit(MakeTask(1, func() {})), ErrPoolIsNotActive)
	require.ErrorIs(t, p.TrySubmit(MakeTask(1, func() {})), ErrPoolIsNotActive)
}

func TestPool_Done(t *testing.T) {
	t.Parallel()

	p, err := NewPool(2, 10)
	require.NoError(t, err)

	done := p.Done()
	select {
	case <-done:
		t.Fatal("Done не должен быть закрыт до Shutdown")
	default:
	}

	require.NoError(t, p.Shutdown(context.Background(), false))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Done должен закрыться после Shutdown")
	}
}

func TestPool_Shutdown_ForceDropsQueuedTasks(t *testing.T) {
	t.Parallel()

	p, err := NewPool(1, 10)
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	require.NoError(t, p.Submit(MakeTask(1, func() { close(started); <-release })))
	<-started

	var executed atomic.Int64
	for i := range 5 {
		require.NoError(t, p.Submit(MakeTask(100+i, func() { executed.Add(1) })))
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.Shutdown(context.Background(), true) }()

	require.Eventually(t, func() bool {
		return p.workers[0].state.Load() == workerStateStoppedForced
	}, time.Second, time.Millisecond)

	close(release)
	require.NoError(t, <-shutdownDone)

	require.Equal(t, int64(0), executed.Load())
	require.Equal(t, 1, p.DoneTasks())
}
