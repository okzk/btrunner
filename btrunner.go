package btrunner

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// BatchingTaskRunner implements a task scheduler to batch multiple same tasks
type BatchingTaskRunner struct {
	taskMap   map[string]*task
	semaphore chan struct{}

	mutex     sync.Mutex
	disposed  bool
	disposing chan struct{}
}

// ErrorAlreadyEnqueued is returned by the EnqueueTask method if
// the task has already been enqueued.
var ErrorAlreadyEnqueued = errors.New("task is already enqueued")

// ErrorDisposed means that BatchingTaskRunner is already disposed.
var ErrorDisposed = errors.New("task runner has been disposed")

type task struct {
	impl     func() error
	callback func(err error)

	signal   chan struct{}
	coolDown time.Duration

	mutex sync.Mutex
	wg    sync.WaitGroup
}

func (b *BatchingTaskRunner) runTaskWorker(t *task) {
	t.wg.Add(1)
	defer t.wg.Done()

	timer := time.NewTimer(time.Hour * 24)
	timer.Stop()
	for {
		if _, ok := <-t.signal; ok {
			select {
			case b.semaphore <- struct{}{}:
				t.mutex.Lock()
				callback := t.callback
				t.callback = nil
				t.mutex.Unlock()

				callback(t.impl())
				<-b.semaphore
			case <-b.disposing:
				t.mutex.Lock()
				callback := t.callback
				t.mutex.Unlock()
				callback(ErrorDisposed)
				return
			}
		} else {
			t.mutex.Lock()
			callback := t.callback
			t.mutex.Unlock()
			if callback != nil {
				callback(ErrorDisposed)
			}
			return
		}

		timer.Reset(t.coolDown)
		select {
		case <-timer.C:
			continue
		case <-b.disposing:
			t.mutex.Lock()
			callback := t.callback
			t.mutex.Unlock()
			if callback != nil {
				callback(ErrorDisposed)
			}
			return
		}
	}
}

// New creates a new BatchingTaskRunner.
func New(concurrency int) *BatchingTaskRunner {
	return &BatchingTaskRunner{
		taskMap:   make(map[string]*task),
		semaphore: make(chan struct{}, concurrency),
		disposing: make(chan struct{}),
	}
}

// Dispose implements graceful disposal. If there are some queued task,
// they are canceled, and their callback function will called with
// ErrorDisposed error.
func (b *BatchingTaskRunner) Dispose() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.disposed = true
	close(b.disposing)

	for _, t := range b.taskMap {
		close(t.signal)
	}
	for _, t := range b.taskMap {
		t.wg.Wait()
	}
}

// RegisterTask registers a new task.
func (b *BatchingTaskRunner) RegisterTask(name string, coolDown time.Duration, impl func() error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.disposed {
		panic("already disposed")
	}
	if _, ok := b.taskMap[name]; ok {
		panic("already registered")
	}

	t := &task{
		impl:     impl,
		coolDown: coolDown,
		signal:   make(chan struct{}, 1),
	}
	go b.runTaskWorker(t)
	b.taskMap[name] = t
}

// EnqueueTask schedules the task execution. If the return value is
// not nil, the callback is never called.
func (b *BatchingTaskRunner) EnqueueTask(name string, callback func(err error)) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.disposed {
		return ErrorDisposed
	}

	t, ok := b.taskMap[name]
	if !ok {
		return fmt.Errorf("no such task [%s]", name)
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.callback != nil {
		return ErrorAlreadyEnqueued
	}
	t.callback = callback
	t.signal <- struct{}{}
	return nil
}
