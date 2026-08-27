// Package pool предоставляет реализацию worker pool (пула воркеров)
// для конкурентного выполнения задач.
//
// Пул создаётся функцией NewPool, задачи отправляются через Submit
// или TrySubmit, а остановка выполняется через Shutdown.
package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Pool управляет набором воркеров, конкурентно выполняющих задачи
// из общей буферизированной очереди.
//
// Pool безопасен для конкурентного использования: размер пула можно
// изменять на лету через SetWorkers, а остановку выполнять через Shutdown.
type Pool struct {
	workers             []*worker
	tasksQueue          chan PoolTask
	submitBreaker       chan struct{}
	workersDone         chan struct{}
	operationsMx        sync.RWMutex
	notActive           atomic.Bool
	workersWG           sync.WaitGroup
	submitWG            sync.WaitGroup
	doneTasksCount      atomic.Int64
	executingTasksCount atomic.Int64
}

var (
	// ErrWrongWorkersCount возвращается, если количество воркеров меньше единицы.
	ErrWrongWorkersCount = errors.New("workers count must be greater than zero")
	// ErrWrongTaskQueueSize возвращается, если размер очереди задач меньше единицы.
	ErrWrongTaskQueueSize = errors.New("task queue size must be greater than zero")
	// ErrPoolIsNotActive возвращается при попытке отправить задачу или изменить
	// размер неактивного (остановленного) пула.
	ErrPoolIsNotActive = errors.New("pool is not active")
	// ErrQueueFull возвращается методом TrySubmit, когда очередь задач заполнена.
	ErrQueueFull = errors.New("queue is full")
	// ErrAlreadyShutdown возвращается при повторном вызове Shutdown.
	ErrAlreadyShutdown = errors.New("already shutting down")
)

// NewPool создаёт пул с заданным числом воркеров и размером очереди задач.
//
// Возвращает ErrWrongWorkersCount, если workersCount меньше единицы, либо
// ErrWrongTaskQueueSize, если taskQueueSize меньше единицы.
func NewPool(workersCount int, taskQueueSize int) (*Pool, error) {
	if workersCount < 1 {
		return nil, ErrWrongWorkersCount
	}
	if taskQueueSize < 1 {
		return nil, ErrWrongTaskQueueSize
	}
	pool := &Pool{
		submitBreaker: make(chan struct{}),
		tasksQueue:    make(chan PoolTask, taskQueueSize),
		workersDone:   make(chan struct{}),
		workers:       make([]*worker, 0, workersCount),
	}
	for range workersCount {
		pool.addWorker()
	}
	return pool, nil
}

// addWorker создаёт воркера, добавляет его в пул и запускает в отдельной горутине.
func (p *Pool) addWorker() {
	w := newWorker(p.tasksQueue, &p.executingTasksCount, &p.doneTasksCount)
	p.workers = append(p.workers, w)
	p.workersWG.Go(
		func() {
			w.run()
		})
}

// Workers возвращает текущее количество воркеров в пуле.
func (p *Pool) Workers() (count int) {
	p.operationsMx.RLock()
	defer p.operationsMx.RUnlock()
	return len(p.workers)
}

// SetWorkers изменяет количество воркеров в пуле на лету.
//
// При уменьшении размера лишние воркеры останавливаются в режиме graceful
// (завершают текущую задачу и прекращают брать новые). При увеличении —
// добавляются новые воркеры. Возвращает ErrWrongWorkersCount, если count
// меньше единицы, либо ErrPoolIsNotActive, если пул уже остановлен.
func (p *Pool) SetWorkers(count int) error {
	if count < 1 {
		return ErrWrongWorkersCount
	}
	p.operationsMx.Lock()
	defer p.operationsMx.Unlock()

	if p.notActive.Load() {
		return ErrPoolIsNotActive
	}

	delta := count - len(p.workers)
	for delta < 0 {
		lastIdx := len(p.workers) - 1
		p.workers[lastIdx].stop(false)
		p.workers = p.workers[:lastIdx]
		delta++
	}
	for delta > 0 {
		p.addWorker()
		delta--
	}

	return nil
}

// ExecutingTasks возвращает количество задач, выполняемых в данный момент.
func (p *Pool) ExecutingTasks() int {
	return int(p.executingTasksCount.Load())
}

// DoneTasks возвращает общее количество завершённых задач.
func (p *Pool) DoneTasks() int {
	return int(p.doneTasksCount.Load())
}

// IsActive возвращает true, пока пул не остановлен вызовом Shutdown.
func (p *Pool) IsActive() bool {
	return !p.notActive.Load()
}

// preSubmit проверяет активность пула и регистрирует входящую отправку задачи.
//
// Регистрация через submitWG гарантирует, что Shutdown не закроет очередь
// задач до завершения всех уже начатых отправок.
func (p *Pool) preSubmit() error {
	p.operationsMx.RLock()
	defer p.operationsMx.RUnlock()
	if p.notActive.Load() {
		return ErrPoolIsNotActive
	}

	p.submitWG.Add(1)
	return nil
}

// Submit отправляет задачу в очередь пула в блокирующем режиме.
//
// Если очередь заполнена, Submit блокируется до появления свободного места
// либо до остановки пула. Возвращает ErrPoolIsNotActive, если пул уже
// остановлен или остановился в процессе ожидания.
func (p *Pool) Submit(task PoolTask, opts ...SubmitOption) error {
	if err := p.preSubmit(); err != nil {
		return err
	}

	defer p.submitWG.Done()

	select {
	case p.tasksQueue <- applySubmitOptions(task, opts):
		return nil
	case <-p.submitBreaker:
		return ErrPoolIsNotActive
	}
}

// TrySubmit отправляет задачу в очередь пула в неблокирующем режиме.
//
// Если очередь заполнена, TrySubmit немедленно возвращает ErrQueueFull.
// Возвращает ErrPoolIsNotActive, если пул уже остановлен.
func (p *Pool) TrySubmit(task PoolTask, opts ...SubmitOption) error {
	if err := p.preSubmit(); err != nil {
		return err
	}

	defer p.submitWG.Done()

	select {
	case p.tasksQueue <- applySubmitOptions(task, opts):
		return nil
	case <-p.submitBreaker:
		return ErrPoolIsNotActive
	default:
		return ErrQueueFull
	}
}

// Shutdown останавливает пул и дожидается завершения воркеров.
//
// Если forceWorkersStop равно false, выполняется graceful shutdown: воркеры
// дорабатывают все задачи из очереди. Если true — воркеры прекращают брать
// новые задачи (текущая задача дорабатывает до конца), а необработанные
// задачи из очереди отбрасываются.
//
// Метод блокируется до полной остановки воркеров либо до отмены ctx;
// в последнем случае возвращается ctx.Err(). Повторный вызов возвращает
// ErrAlreadyShutdown.
func (p *Pool) Shutdown(ctx context.Context, forceWorkersStop bool) error {
	p.operationsMx.Lock()
	if !p.notActive.CompareAndSwap(false, true) {
		defer p.operationsMx.Unlock()
		return ErrAlreadyShutdown
	}
	close(p.submitBreaker)

	p.operationsMx.Unlock()

	go func() {
		p.submitWG.Wait()
		close(p.tasksQueue)
	}()

	go func() {
		p.workersWG.Wait()
		close(p.workersDone)
	}()

	if forceWorkersStop {
		p.operationsMx.RLock()
		for idx := range p.workers {
			p.workers[idx].stop(true)
		}
		p.operationsMx.RUnlock()

		//вычитываем остатки задачек
		drainDone := make(chan struct{})
		go func() {
			for range p.tasksQueue {
			}
			close(drainDone)
		}()
		select {
		case <-drainDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	select {
	case <-p.workersDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done возвращает канал, который закрывается после полной остановки
// всех воркеров пула.
func (p *Pool) Done() <-chan struct{} {
	return p.workersDone
}
