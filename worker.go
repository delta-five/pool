package pool

import "sync/atomic"

type worker struct {
	workerCloseCh <-chan struct{}
	taskCh        <-chan Task
	statistic     *statisticHolder
	panicHandler  *atomic.Pointer[func(recovered any)]
}

func makeWorker(pool *Pool) worker {
	return worker{
		workerCloseCh: pool.workerCloseCh,
		taskCh:        pool.taskCh,
		statistic:     &pool.statistic,
		panicHandler:  &pool.panicHandler,
	}
}

func (w worker) run() {
	for {
		select {
		case <-w.workerCloseCh:
			return
		default:
		}

		select {
		case <-w.workerCloseCh:
			return
		case task, ok := <-w.taskCh:
			if !ok {
				return
			}
			w.doTask(task)
		}
	}
}

func (w worker) doTask(task Task) {
	w.statistic.workersActive.Add(1)
	defer func() {
		r := recover()
		if r != nil {
			w.statistic.panicsCount.Add(1)
			if h := w.panicHandler.Load(); h != nil {
				(*h)(r)
			}
		}
		w.statistic.taskProcessed.Add(1)
		w.statistic.workersActive.Add(-1)
	}()

	task()
}
