# pool

Реализация worker pool (пула воркеров) для конкурентного выполнения задач на Go.

## Установка

```bash
go get github.com/delta-five/pool
```

## Возможности

- Фиксированный пул воркеров с возможностью динамического изменения размера (`SetWorkers`)
- Буферизированная очередь задач
- Два режима отправки задачи: блокирующий (`Submit`) и неблокирующий (`TrySubmit`)
- Обработка паник через `WithPanicHandler` — опция при отправке задачи
- Graceful shutdown с ожиданием завершения очереди либо с отбрасыванием очереди
- Ожидание полного завершения пула через `Done()`
- Счётчики выполненных и выполняющихся задач

## Быстрый старт

```go
package main

import (
	"context"
	"fmt"

	"github.com/delta-five/pool"
)

func main() {
	// 4 воркера, очередь на 100 задач
	p, err := pool.NewPool(4, 100)
	if err != nil {
		panic(err)
	}

	for i := range 10 {
		n := i
		err := p.Submit(pool.MakeTask(n, func() {
			fmt.Printf("выполняю задачу %d\n", n)
		}))
		if err != nil {
			fmt.Println("отправка не удалась:", err)
		}
	}

	// Ждём завершения всех задач в очереди и останавливаем пул.
	if err := p.Shutdown(context.Background(), false); err != nil {
		fmt.Println("shutdown:", err)
	}
}
```

## Создание пула

```go
p, err := pool.NewPool(workersCount, taskQueueSize)
```

- `workersCount` — количество воркеров (больше нуля)
- `taskQueueSize` — размер очереди задач (больше нуля)

Возвращает ошибку `ErrWrongWorkersCount` или `ErrWrongTaskQueueSize` при некорректных аргументах.

## Задачи

Задача реализует интерфейс `PoolTask`:

```go
type PoolTask interface {
	ID() any
	Do()
}
```

Задачу удобно создавать через обобщённую функцию `MakeTask`, которая принимает
идентификатор и функцию выполнения:

```go
err := p.Submit(pool.MakeTask("task-id", func() {
	// полезная работа
}))
```

### Обработка паник

По умолчанию паника внутри задачи распространяется наружу и приводит к падению
программы (crash-by-design). Чтобы паника не роняла процесс, передайте обработчик
через опцию `WithPanicHandler` — воркер автоматически восстановит выполнение и
продолжит обработку следующих задач:

```go
err := p.Submit(
	pool.MakeTask("task-id", func() {
		panic("что-то пошло не так")
	}),
	pool.WithPanicHandler(func(id any, recovered any) {
		fmt.Printf("паника в задаче %v: %v\n", id, recovered)
	}),
)
```

Опция `WithPanicHandler` доступна как для `Submit`, так и для `TrySubmit`.
Если обработчик не передан — поведение по умолчанию (crash-by-design).

> **Важно:** сам обработчик не должен паниковать. Паника внутри обработчика
> распространяется наружу и роняет процесс.

## Отправка задач

### Submit — блокирующая отправка

`Submit` блокируется, если очередь заполнена, до появления свободного места либо
до остановки пула:

```go
err := p.Submit(task)
```

Опциональные настройки передаются через функциональные опции, например
`WithPanicHandler`:

```go
err := p.Submit(task, pool.WithPanicHandler(func(id any, recovered any) {
	log.Printf("паника в задаче %v: %v", id, recovered)
}))
```

### TrySubmit — неблокирующая отправка

`TrySubmit` возвращает `ErrQueueFull`, если очередь заполнена:

```go
err := p.TrySubmit(task)
if err == pool.ErrQueueFull {
	// очередь заполнена
}
```

## Изменение числа воркеров

```go
// увеличить до 8 воркеров
if err := p.SetWorkers(8); err != nil {
	// ...
}

// уменьшить до 2 воркеров
if err := p.SetWorkers(2); err != nil {
	// ...
}

fmt.Println("текущее число воркеров:", p.Workers())
```

## Наблюдение за состоянием

```go
fmt.Println("выполняется сейчас:", p.ExecutingTasks())
fmt.Println("выполнено всего:", p.DoneTasks())
fmt.Println("состояние активности:", p.IsActive())
```

## Остановка пула

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// forceWorkersStop = false: дождаться выполнения всех задач из очереди
err := p.Shutdown(ctx, false)
```

Либо, чтобы остановиться быстрее и отбросить необработанные задачи из очереди:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// forceWorkersStop = true: отбросить задачи из очереди
err := p.Shutdown(ctx, true)
```

После остановки `Submit`/`TrySubmit` возвращают `ErrPoolIsNotActive`,
повторный `Shutdown` — `ErrAlreadyShutdown`, а `SetWorkers` — `ErrPoolIsNotActive`.

### Done — ожидание завершения пула

`Done` возвращает канал, который закрывается, когда все воркеры завершили работу.
Это полезно, если `Shutdown` превысил дедлайн контекста и вы хотите дождаться
полной остановки асинхронно:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := p.Shutdown(ctx, true); err != nil {
	fmt.Println("shutdown превысил дедлайн, ждём в фоне:", err)
}

// Дождаться полного завершения всех воркеров.
<-p.Done()
fmt.Println("пул полностью остановлен")
```

## Полный пример

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/delta-five/pool"
)

func main() {
	p, err := pool.NewPool(4, 100)
	if err != nil {
		log.Fatal(err)
	}

	// Отправляем 20 задач.
	for i := range 20 {
		if err := p.Submit(pool.MakeTask(i, func() {
			fmt.Printf("задача %d стартовала\n", i)
			time.Sleep(50 * time.Millisecond)
			fmt.Printf("задача %d завершена\n", i)
		})); err != nil {
			log.Printf("submit %d: %v", i, err)
		}
	}

	// Меняем число воркеров на лету.
	if err := p.SetWorkers(8); err != nil {
		log.Printf("SetWorkers: %v", err)
	}

	// Graceful shutdown: ждём выполнения всей очереди.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx, false); err != nil {
		log.Printf("shutdown: %v", err)
	}

	fmt.Printf("выполнено задач: %d\n", p.DoneTasks())
}
```

## Тесты

```bash
go test -race -cover ./...
```
