package pool

import (
	"errors"
	"sync"
	"sync/atomic"
)

type Task func()

type Pool struct {
	shutdownCh    chan struct{}
	doneCh        chan struct{}
	workerCloseCh chan struct{}
	workersCount  int
	taskCh        chan Task
	statistic     poolStatisticHolder
	wg            sync.WaitGroup
	lock          sync.RWMutex
	stopCalled    bool
}

type poolStatisticHolder struct {
	taskProcessed atomic.Int64
	workersActive atomic.Int64
	panicsCount   atomic.Int64
}

type poolStatistic struct {
	TaskProcessed int64
	WorkersActive int64
	PanicsCount   int64
}

func NewPool(workersCount int, queueSize int) (*Pool, error) {
	if workersCount < 0 {
		return nil, errors.New("workers count must be non-negative")
	}
	if queueSize < 0 {
		return nil, errors.New("queue size must be non-negative")
	}

	pool := &Pool{
		doneCh:        make(chan struct{}),
		shutdownCh:    make(chan struct{}),
		workerCloseCh: make(chan struct{}),
		taskCh:        make(chan Task, queueSize),
		workersCount:  workersCount,
	}

	go func() {
		<-pool.shutdownCh
		pool.wg.Wait()
		close(pool.doneCh)
	}()

	for range workersCount {
		pool.addWorker()
	}

	return pool, nil
}

func (p *Pool) addWorker() {
	w := makeWorker(p)
	p.wg.Go(w.run)
}

var errPoolNotRunning = errors.New("pool is not running")

func (p *Pool) Submit(task Task) error {
	p.lock.RLock()
	if p.stopCalled {
		p.lock.RUnlock()
		return errPoolNotRunning
	}
	p.lock.RUnlock()

	select {
	case p.taskCh <- task:
		return nil
	case <-p.shutdownCh:
		return errPoolNotRunning
	}
}

var errQueueFull = errors.New("task queue is full")

func (p *Pool) TrySubmit(task Task) error {
	p.lock.RLock()
	if p.stopCalled {
		p.lock.RUnlock()
		return errPoolNotRunning
	}
	p.lock.RUnlock()

	select {
	case p.taskCh <- task:
		return nil
	case <-p.shutdownCh:
		return errPoolNotRunning
	default:
		return errQueueFull
	}
}

func (p *Pool) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopCalled {
		return errPoolNotRunning
	}
	p.stopCalled = true
	close(p.shutdownCh)

	close(p.taskCh)
	p.taskCh = nil

	close(p.workerCloseCh)
	p.workerCloseCh = nil

	return nil
}

func (p *Pool) Done() <-chan struct{} {
	return p.doneCh
}

func (p *Pool) SetWorkersCount(count int) error {
	if count < 0 {
		return errors.New("workers count must be non-negative")
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopCalled {
		return errPoolNotRunning
	}

	delta := count - p.workersCount
	p.workersCount = count

	for delta > 0 {
		p.addWorker()
		delta--
	}

	if delta < 0 {
		go func(pl *Pool, d int) {
			for d < 0 {
				pl.lock.RLock()
				if pl.stopCalled {
					pl.lock.RUnlock()
					return
				}
				pl.lock.RUnlock()
				select {
				case pl.workerCloseCh <- struct{}{}:
				case <-pl.shutdownCh:
					return
				}
				d++
			}
		}(p, delta)
	}
	return nil
}

func (p *Pool) Statistic() poolStatistic {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return poolStatistic{
		TaskProcessed: p.statistic.taskProcessed.Load(),
		WorkersActive: p.statistic.workersActive.Load(),
		PanicsCount:   p.statistic.panicsCount.Load(),
	}
}
