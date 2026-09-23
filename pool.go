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
	workersWG     sync.WaitGroup
	submitWG      sync.WaitGroup
	lock          sync.RWMutex
	stopCalled    atomic.Bool
}

type poolStatisticHolder struct {
	taskProcessed atomic.Int64
	workersActive atomic.Int64
	panicsCount   atomic.Int64
}

type PoolStatistic struct {
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
		pool.workersWG.Wait()
		close(pool.doneCh)
	}()

	for range workersCount {
		pool.addWorker()
	}

	return pool, nil
}

func (p *Pool) addWorker() {
	w := makeWorker(p)
	p.workersWG.Go(w.run)
}

func (p *Pool) killWorker() bool {
	p.submitWG.Add(1)
	defer p.submitWG.Done()

	if p.stopCalled.Load() {
		return false
	}

	select {
	case p.workerCloseCh <- struct{}{}:
		return true
	case <-p.shutdownCh:
		return false
	}
}

var errPoolNotRunning = errors.New("pool is not running")

func (p *Pool) Submit(task Task) error {
	p.submitWG.Add(1)
	defer p.submitWG.Done()

	if p.stopCalled.Load() {
		return errPoolNotRunning
	}

	select {
	case p.taskCh <- task:
		return nil
	case <-p.shutdownCh:
		return errPoolNotRunning
	}
}

var errQueueFull = errors.New("task queue is full")

func (p *Pool) TrySubmit(task Task) error {
	p.submitWG.Add(1)
	defer p.submitWG.Done()

	if p.stopCalled.Load() {
		return errPoolNotRunning
	}

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
	if !p.stopCalled.CompareAndSwap(false, true) {
		return errPoolNotRunning
	}
	close(p.shutdownCh)

	p.submitWG.Wait()
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

	p.submitWG.Add(1)
	defer p.submitWG.Done()

	if p.stopCalled.Load() {
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
				if pl.killWorker() {
					return
				}
				d++
			}
		}(p, delta)
	}
	return nil
}

func (p *Pool) Statistic() PoolStatistic {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return PoolStatistic{
		TaskProcessed: p.statistic.taskProcessed.Load(),
		WorkersActive: p.statistic.workersActive.Load(),
		PanicsCount:   p.statistic.panicsCount.Load(),
	}
}
