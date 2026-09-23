package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorker_DoTask_Success(t *testing.T) {
	pool := &Pool{}
	w := makeWorker(pool)

	ran := false
	w.doTask(func() { ran = true })

	assert.True(t, ran)
	assert.Equal(t, int64(1), pool.statistic.taskProcessed.Load())
	assert.Equal(t, int64(0), pool.statistic.panicsCount.Load())
}

func TestWorker_DoTask_ActiveCounterReturnsToZero(t *testing.T) {
	pool := &Pool{}
	w := makeWorker(pool)

	w.doTask(func() {})

	assert.Equal(t, int64(0), pool.statistic.workersActive.Load())
}

func TestWorker_DoTask_PanicIsRecovered(t *testing.T) {
	pool := &Pool{}
	w := makeWorker(pool)

	require.NotPanics(t, func() {
		w.doTask(func() { panic("boom") })
	})

	assert.Equal(t, int64(1), pool.statistic.panicsCount.Load())
	assert.Equal(t, int64(1), pool.statistic.taskProcessed.Load())
}

func TestWorker_DoTask_SurvivesPanicAndProcessesNextTask(t *testing.T) {
	pool := &Pool{}
	w := makeWorker(pool)

	w.doTask(func() { panic("boom") })

	ran := false
	w.doTask(func() { ran = true })

	assert.True(t, ran)
	assert.Equal(t, int64(2), pool.statistic.taskProcessed.Load())
	assert.Equal(t, int64(1), pool.statistic.panicsCount.Load())
}

func TestWorker_Run_ProcessesQueuedTasksAndExitsOnClose(t *testing.T) {
	pool := &Pool{
		taskCh:        make(chan Task, 2),
		workerCloseCh: make(chan struct{}),
	}
	w := makeWorker(pool)

	runDone := make(chan struct{})
	go func() {
		w.run()
		close(runDone)
	}()

	results := make(chan int, 2)
	pool.taskCh <- func() { results <- 1 }
	pool.taskCh <- func() { results <- 2 }

	got := map[int]bool{}
	for range 2 {
		select {
		case v := <-results:
			got[v] = true
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for task execution")
		}
	}
	assert.True(t, got[1])
	assert.True(t, got[2])
	assert.Equal(t, int64(2), pool.statistic.taskProcessed.Load())

	close(pool.workerCloseCh)
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for worker to exit after workerCloseCh was closed")
	}
}

func TestWorker_Run_ExitsWhenTaskChannelIsClosed(t *testing.T) {
	pool := &Pool{
		taskCh:        make(chan Task),
		workerCloseCh: make(chan struct{}),
	}
	w := makeWorker(pool)

	runDone := make(chan struct{})
	go func() {
		w.run()
		close(runDone)
	}()

	close(pool.taskCh)

	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for worker to exit after taskCh was closed")
	}
}

func TestWorker_Run_ExitsImmediatelyWhenAlreadyClosed(t *testing.T) {
	pool := &Pool{
		taskCh:        make(chan Task),
		workerCloseCh: make(chan struct{}),
	}
	close(pool.workerCloseCh)
	w := makeWorker(pool)

	runDone := make(chan struct{})
	go func() {
		w.run()
		close(runDone)
	}()

	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for worker to exit on an already-closed workerCloseCh")
	}
}
