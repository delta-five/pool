package pool

import (
	"errors"
	"sync"
	"sync/atomic"
)

type Task func()

// Pool — пул воркеров для конкурентного выполнения задач.
type Pool struct {
	shutdownCh    chan struct{}
	doneCh        chan struct{}
	workerCloseCh chan struct{}
	workersCount  int
	taskCh        chan Task
	statistic     poolStatisticHolder
	panicHandler  atomic.Pointer[func(recovered any)]
	workersWG     sync.WaitGroup
	submitWG      sync.WaitGroup
	lock          sync.RWMutex
	stopCalled    bool
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

// ErrInvalidWorkersCount возвращается NewPool и SetWorkersCount при отрицательном числе воркеров.
var ErrInvalidWorkersCount = errors.New("workers count must be non-negative")

// ErrInvalidQueueSize возвращается NewPool при отрицательном размере очереди задач.
var ErrInvalidQueueSize = errors.New("queue size must be non-negative")

func NewPool(workersCount int, queueSize int) (*Pool, error) {
	if workersCount < 0 {
		return nil, ErrInvalidWorkersCount
	}
	if queueSize < 0 {
		return nil, ErrInvalidQueueSize
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
	p.lock.RLock()
	if p.stopCalled {
		p.lock.RUnlock()
		return false
	}
	p.submitWG.Add(1)
	p.lock.RUnlock()
	defer p.submitWG.Done()

	select {
	case p.workerCloseCh <- struct{}{}:
		return true
	case <-p.shutdownCh:
		return false
	}
}

// ErrPoolNotRunning возвращается Submit, TrySubmit и SetWorkersCount после остановки пула,
// а также самим Stop при повторном вызове.
var ErrPoolNotRunning = errors.New("pool is not running")

func (p *Pool) Submit(task Task) error {
	p.lock.RLock()
	p.submitWG.Add(1)
	defer p.submitWG.Done()

	if p.stopCalled {
		p.lock.RUnlock()
		return ErrPoolNotRunning
	}
	p.lock.RUnlock()

	select {
	case p.taskCh <- task:
		return nil
	case <-p.shutdownCh:
		return ErrPoolNotRunning
	}
}

// ErrQueueFull возвращается TrySubmit, когда в очереди задач нет свободного места.
var ErrQueueFull = errors.New("task queue is full")

func (p *Pool) TrySubmit(task Task) error {
	p.lock.RLock()
	p.submitWG.Add(1)
	defer p.submitWG.Done()

	if p.stopCalled {
		p.lock.RUnlock()
		return ErrPoolNotRunning
	}
	p.lock.RUnlock()

	select {
	case p.taskCh <- task:
		return nil
	case <-p.shutdownCh:
		return ErrPoolNotRunning
	default:
		return ErrQueueFull
	}
}

func (p *Pool) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.stopCalled {
		return ErrPoolNotRunning
	}
	p.stopCalled = true
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
		return ErrInvalidWorkersCount
	}

	p.lock.Lock()
	p.submitWG.Add(1)
	defer p.submitWG.Done()
	defer p.lock.Unlock()

	if p.stopCalled {
		return ErrPoolNotRunning
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
				if !pl.killWorker() {
					return
				}
				d++
			}
		}(p, delta)
	}
	return nil
}

// Statistic возвращает снимок счётчиков пула. Вызов никогда не блокируется,
// в том числе пока выполняется Stop.
func (p *Pool) Statistic() PoolStatistic {
	return PoolStatistic{
		TaskProcessed: p.statistic.taskProcessed.Load(),
		WorkersActive: p.statistic.workersActive.Load(),
		PanicsCount:   p.statistic.panicsCount.Load(),
	}
}

// OnPanic регистрирует колбэк, который вызывается при панике внутри задачи —
// в той же горутине, что выполняла задачу. Паника в любом случае
// перехватывается автоматически, независимо от того, задан ли колбэк:
// счётчик PanicsCount увеличивается, а пул продолжает работать. Колбэк нужен
// только для дополнительного наблюдения (например, логирования). nil снимает
// колбэк.
//
// Сам колбэк не должен паниковать: паника внутри него распространится дальше
// и приведёт к падению процесса.
func (p *Pool) OnPanic(handler func(recovered any)) {
	if handler == nil {
		p.panicHandler.Store(nil)
		return
	}
	p.panicHandler.Store(&handler)
}
