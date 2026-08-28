package pool

import (
	"sync/atomic"
)

// Состояния воркера.
const (
	workerStateNew             int32 = iota // воркер активен
	workerStateStoppedGraceful              // воркер остановлен в режиме graceful
	workerStateStoppedForced                // воркер остановлен принудительно
)

// worker — воркер пула, обрабатывающий задачи из общей очереди.
type worker struct {
	tasks               chan PoolTask
	breaker             chan struct{}
	state               atomic.Int32
	doneTasksCount      *atomic.Int64
	executingTasksCount *atomic.Int64
}

// newWorker создаёт воркера, разделяющего очередь задач и счётчики пула.
func newWorker(
	tasks chan PoolTask, executingTasksCount *atomic.Int64, doneTasksCount *atomic.Int64,
) *worker {
	w := &worker{
		tasks:               tasks,
		breaker:             make(chan struct{}),
		doneTasksCount:      doneTasksCount,
		executingTasksCount: executingTasksCount,
	}

	return w
}

// run запускает цикл обработки задач.
//
// Воркер читает задачи из очереди и выполняет их, обновляя счётчики
// выполняющихся и завершённых задач. Цикл завершается при остановке воркера
// (через breaker) или при закрытии очереди задач.
func (w *worker) run() {
	for {
		if w.state.Load() != workerStateNew {
			return
		}
		select {
		case task, ok := <-w.tasks:
			if !ok {
				return
			}
			if w.state.Load() == workerStateStoppedForced { // не выполнять после запроса на останов
				continue
			}
			func() {
				w.executingTasksCount.Add(1)
				defer w.doneTasksCount.Add(1)
				defer w.executingTasksCount.Add(-1)
				// panic recover и context checking по умолчанию crash-by-design,
				// но могут быть включены через WithPanicHandler при отправке задачи.
				task.Do()
			}()
		case <-w.breaker:
			return
		}
	}
}

// stop переводит воркера в состояние остановки.
//
// Если shutdownMode равно false, воркер останавливается в режиме graceful;
// если true — принудительно. Повторный вызов игнорируется.
func (w *worker) stop(shutdownMode bool) {
	newState := workerStateStoppedGraceful
	if shutdownMode {
		newState = workerStateStoppedForced
	}

	if !w.state.CompareAndSwap(workerStateNew, newState) {
		return
	}
	close(w.breaker)
}
