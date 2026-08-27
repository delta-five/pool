package pool

// PoolTask представляет задачу, выполняемую воркером пула.
type PoolTask interface {
	// ID возвращает идентификатор задачи.
	ID() any
	// Do выполняет полезную работу задачи.
	Do()
}

// workerTask — типовая реализация PoolTask, хранящая идентификатор
// и функцию выполнения.
type workerTask[T any] struct {
	id T
	fn func()
}

// MakeTask создаёт задачу PoolTask из идентификатора и функции выполнения.
func MakeTask[T any](id T, fn func()) PoolTask {
	return workerTask[T]{
		id: id,
		fn: fn,
	}
}

// ID возвращает идентификатор задачи.
func (t workerTask[T]) ID() any {
	return t.id
}

// Do выполняет функцию задачи.
func (t workerTask[T]) Do() {
	t.fn()
}

// SubmitOption настраивает поведение отправки задачи.
type SubmitOption func(*submitConfig)

// panicHandlerFunc — обработчик паники, возникающей внутри задачи.
// Принимает идентификатор задачи и значение, переданное в panic.
type panicHandlerFunc func(id any, recovered any)

// submitConfig хранит настройки, применяемые к задаче при отправке.
type submitConfig struct {
	panicHandler panicHandlerFunc
}

// WithPanicHandler устанавливает обработчик паники для задачи.
// При панике внутри задачи восстановление выполняется автоматически,
// а значение паники передаётся в handler. Воркер при этом не завершается.
func WithPanicHandler(handler panicHandlerFunc) SubmitOption {
	return func(cfg *submitConfig) {
		cfg.panicHandler = handler
	}
}

// applySubmitOptions применяет опции отправки к задаче и возвращает итоговую
// задачу. Если задан обработчик паники, задача оборачивается в recover,
// передающий обработчику идентификатор задачи и значение паники.
func applySubmitOptions(task PoolTask, opts []SubmitOption) PoolTask {
	if len(opts) == 0 {
		return task
	}
	cfg := &submitConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.panicHandler == nil {
		return task
	}
	handler := cfg.panicHandler

	return MakeTask[any](
		task.ID(),
		func() {
			defer func() {
				if r := recover(); r != nil {
					handler(task.ID(), r)
				}
			}()
			task.Do()
		})
}
