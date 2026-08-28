package pool

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorker_run_ReturnsWhenAlreadyStopped(t *testing.T) {
	t.Parallel()

	var executing, done atomic.Int64
	w := newWorker(make(chan PoolTask, 1), &executing, &done)
	w.state.Store(workerStateStoppedGraceful)

	finished := make(chan struct{})
	go func() {
		w.run()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("run должен завершиться, если воркер уже остановлен")
	}
}

func TestWorker_run_ReturnsOnClosedQueue(t *testing.T) {
	t.Parallel()

	var executing, done atomic.Int64
	tasks := make(chan PoolTask, 1)
	w := newWorker(tasks, &executing, &done)
	close(tasks)

	finished := make(chan struct{})
	go func() {
		w.run()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("run должен завершиться при закрытой очереди")
	}
}

func TestWorker_run_ReturnsOnBreaker(t *testing.T) {
	t.Parallel()

	var executing, done atomic.Int64
	tasks := make(chan PoolTask, 1)
	w := newWorker(tasks, &executing, &done)
	close(w.breaker)

	finished := make(chan struct{})
	go func() {
		w.run()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("run должен завершиться при закрытом breaker")
	}
}

func TestWorker_run_ExecutesTask(t *testing.T) {
	t.Parallel()

	var executing, done atomic.Int64
	tasks := make(chan PoolTask, 1)
	w := newWorker(tasks, &executing, &done)

	executed := false
	tasks <- MakeTask(1, func() { executed = true })
	close(tasks)

	finished := make(chan struct{})
	go func() {
		w.run()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("run должен завершиться")
	}

	require.True(t, executed)
	require.Equal(t, int64(1), done.Load())
	require.Equal(t, int64(0), executing.Load())
}

func TestWorker_run_SkipsTaskWhenStoppedForced(t *testing.T) {
	t.Parallel()

	var executing, done atomic.Int64
	tasks := make(chan PoolTask, 1)
	w := newWorker(tasks, &executing, &done)

	finished := make(chan struct{})
	go func() {
		w.run()
		close(finished)
	}()

	// Прогреваем воркер, чтобы он гарантированно оказался в цикле обработки.
	tasks <- MakeTask("warmup", func() {})
	require.Eventually(t, func() bool { return done.Load() == 1 }, time.Second, time.Millisecond)

	// Даём воркеру гарантированно вернуться в select: после завершения
	// warmup остаётся лишь пара атомарных операций до блокировки на select.
	time.Sleep(time.Millisecond)

	// Переводим воркер в forced без закрытия breaker и кладём задачу:
	// select выберет задачу, а проверка состояния её пропустит.
	w.state.Store(workerStateStoppedForced)
	tasks <- MakeTask("dropped", func() { t.Error("задача не должна выполниться") })

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("run должен завершиться")
	}

	require.Equal(t, int64(1), done.Load())
	require.Equal(t, int64(0), executing.Load())
	require.Empty(t, tasks)
}

func TestWorker_stop(t *testing.T) {
	t.Parallel()

	t.Run("graceful", func(t *testing.T) {
		t.Parallel()

		var executing, done atomic.Int64
		w := newWorker(make(chan PoolTask, 1), &executing, &done)

		w.stop(false)

		require.Equal(t, workerStateStoppedGraceful, w.state.Load())
		select {
		case <-w.breaker:
		default:
			t.Fatal("breaker должен быть закрыт")
		}
	})

	t.Run("forced", func(t *testing.T) {
		t.Parallel()

		var executing, done atomic.Int64
		w := newWorker(make(chan PoolTask, 1), &executing, &done)

		w.stop(true)

		require.Equal(t, workerStateStoppedForced, w.state.Load())
		select {
		case <-w.breaker:
		default:
			t.Fatal("breaker должен быть закрыт")
		}
	})

	t.Run("повторный вызов игнорируется", func(t *testing.T) {
		t.Parallel()

		var executing, done atomic.Int64
		w := newWorker(make(chan PoolTask, 1), &executing, &done)

		w.state.Store(workerStateStoppedGraceful)
		w.stop(false)

		require.Equal(t, workerStateStoppedGraceful, w.state.Load())
		select {
		case <-w.breaker:
			t.Fatal("breaker не должен быть закрыт при повторном вызове")
		default:
		}
	})
}
