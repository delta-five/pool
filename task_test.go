package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeTask(t *testing.T) {
	t.Parallel()

	id := "task-id"
	fnCalled := false

	task := MakeTask(id, func() { fnCalled = true })

	require.Equal(t, id, task.ID())

	task.Do()
	require.True(t, fnCalled)
}

func TestWorkerTask_ID(t *testing.T) {
	t.Parallel()

	task := workerTask[int]{id: 42, fn: func() {}}

	require.Equal(t, 42, task.ID())
}

func TestWorkerTask_Do(t *testing.T) {
	t.Parallel()

	called := false
	task := workerTask[string]{id: "x", fn: func() { called = true }}

	task.Do()

	require.True(t, called)
}

func TestWithPanicHandler(t *testing.T) {
	t.Parallel()

	handler := panicHandlerFunc(func(id any, recovered any) {})

	cfg := &submitConfig{}
	WithPanicHandler(handler)(cfg)

	require.NotNil(t, cfg.panicHandler)
}

func TestApplySubmitOptions(t *testing.T) {
	t.Parallel()

	t.Run("без опций возвращает исходную задачу", func(t *testing.T) {
		t.Parallel()

		called := false
		task := MakeTask("id", func() { called = true })
		got := applySubmitOptions(task, nil)

		require.Equal(t, "id", got.ID())
		got.Do()
		require.True(t, called)
	})

	t.Run("опция без обработчика возвращает исходную задачу", func(t *testing.T) {
		t.Parallel()

		called := false
		task := MakeTask("id", func() { called = true })
		noop := SubmitOption(func(_ *submitConfig) {})
		got := applySubmitOptions(task, []SubmitOption{noop})

		require.Equal(t, "id", got.ID())
		got.Do()
		require.True(t, called)
	})

	t.Run("с обработчиком оборачивает задачу и сохраняет ID", func(t *testing.T) {
		t.Parallel()

		done := false
		task := MakeTask("id", func() { done = true })

		got := applySubmitOptions(task, []SubmitOption{WithPanicHandler(func(id any, recovered any) {})})

		require.Equal(t, "id", got.ID())
		got.Do()
		require.True(t, done)
	})

	t.Run("обработчик вызывается при панике задачи", func(t *testing.T) {
		t.Parallel()

		task := MakeTask("id", func() { panic("boom") })

		var gotID any
		var gotRecovered any
		got := applySubmitOptions(task, []SubmitOption{WithPanicHandler(func(id any, recovered any) {
			gotID = id
			gotRecovered = recovered
		})})

		require.NotPanics(t, func() { got.Do() })
		require.Equal(t, "id", gotID)
		require.Equal(t, "boom", gotRecovered)
	})
}
