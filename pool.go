package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Task - функция, которую выполняет пул.
type Task func()

// Pool - пул воркеров для конкурентного выполнения задач.
type Pool struct {
	shutdownCh    chan struct{}
	doneCh        chan struct{}
	workerCloseCh chan struct{}
	workersCount  int
	taskCh        chan Task
	statistic     statisticHolder
	panicHandler  atomic.Pointer[func(recovered any)]
	workersWG     sync.WaitGroup
	submitWG      sync.WaitGroup
	lock          sync.RWMutex
	stopCalled    bool
}

type statisticHolder struct {
	taskProcessed atomic.Int64
	panicsCount   atomic.Int64
	workersActive atomic.Int64
}

// Statistic - снимок счётчиков пула, возвращаемый Statistic.
type Statistic struct {
	// TaskProcessed - число обработанных задач (успешно завершённых или с паникой).
	TaskProcessed int64
	// WorkersActive - число воркеров, выполняющих задачу прямо сейчас.
	WorkersActive int64
	// PanicsCount - число перехваченных паник внутри задач.
	PanicsCount int64
	// QueueLen - длина очереди задач
	QueueLen int
}

// ErrInvalidWorkersCount возвращается NewPool и SetWorkersCount при отрицательном числе воркеров.
var ErrInvalidWorkersCount = errors.New("workers count must be non-negative")

// ErrInvalidQueueSize возвращается NewPool при отрицательном размере очереди задач.
var ErrInvalidQueueSize = errors.New("queue size must be non-negative")

// NewPool создаёт пул с заданным числом воркеров и размером очереди задач.
// workersCount и queueSize должны быть неотрицательными, иначе возвращается
// ErrInvalidWorkersCount или ErrInvalidQueueSize.
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

// Submit отправляет задачу в очередь. Блокируется, если очередь заполнена,
// до появления свободного места либо до остановки пула - в последнем случае
// возвращает ErrPoolNotRunning.
func (p *Pool) Submit(task Task) error {
	return p.SubmitContext(context.Background(), task)
}

// SubmitContext отправляет задачу в очередь, как и Submit, но дополнительно
// прерывает ожидание по отмене контекста, возвращая ctx.Err(). Если пул уже
// остановлен, возвращает ErrPoolNotRunning.
func (p *Pool) SubmitContext(ctx context.Context, task Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}

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
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ErrQueueFull возвращается TrySubmit, когда в очереди задач нет свободного места.
var ErrQueueFull = errors.New("task queue is full")

// TrySubmit отправляет задачу в очередь, не блокируясь: если очередь
// заполнена, сразу возвращает ErrQueueFull. Если пул уже остановлен,
// возвращает ErrPoolNotRunning.
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

// Stop останавливает пул: перестаёт принимать новые задачи и передаёт всем
// воркерам сигнал завершения. Сама по себе не ждёт завершения уже
// выполняющихся задач - для этого используйте Done. dropQueued решает судьбу
// задач, которые к моменту вызова уже лежат в очереди, но ещё не начали
// выполняться: false - воркеры дорабатывают их все, прежде чем завершиться;
// true - они отбрасываются, воркеры завершаются, как только освобождаются от
// текущей задачи. Повторный вызов возвращает ErrPoolNotRunning.
func (p *Pool) Stop(dropQueued bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.stopCalled {
		return ErrPoolNotRunning
	}
	p.stopCalled = true
	close(p.shutdownCh)

	p.submitWG.Wait()

	if dropQueued {
		close(p.workerCloseCh)
	}

	close(p.taskCh)
	p.taskCh = nil

	return nil
}

// Done возвращает канал, который закрывается, когда все воркеры полностью
// завершили работу после Stop.
func (p *Pool) Done() <-chan struct{} {
	return p.doneCh
}

// WorkersCount возвращает текущее число воркеров пула.
func (p *Pool) WorkersCount() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.workersCount
}

// SetWorkersCount изменяет число воркеров пула. Увеличение применяется
// сразу; уменьшение - асинхронно, по мере того как воркеры освобождаются от
// текущей задачи (метод не ждёт этого завершения). Отрицательное count
// возвращает ErrInvalidWorkersCount, вызов после остановки пула -
// ErrPoolNotRunning.
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
func (p *Pool) Statistic() Statistic {
	return Statistic{
		TaskProcessed: p.statistic.taskProcessed.Load(),
		WorkersActive: p.statistic.workersActive.Load(),
		PanicsCount:   p.statistic.panicsCount.Load(),
		QueueLen:      len(p.taskCh),
	}
}

// OnPanic регистрирует колбэк, который вызывается при панике внутри задачи -
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
