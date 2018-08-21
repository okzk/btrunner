package btrunner

import (
	"sync"
	"testing"
	"time"
)

const testJobTimeUnit = time.Millisecond * 100

func taskImpl() error {
	time.Sleep(testJobTimeUnit)
	return nil
}

func neverCall(err error) {
	panic(err)
}

func TestSingleTask(t *testing.T) {
	runner := New(2)
	runner.RegisterTask("t", testJobTimeUnit, taskImpl)

	count := 0
	startAt := time.Now()

	// enqueue 1st task:
	//  - task is not running
	//  - queue is empty
	err := runner.EnqueueTask("t", func(err error) {
		count++
		if count != 1 {
			t.Fatalf("callback count should be %d, actual %d", 1, count)
		}
	})
	if err != nil {
		t.Fatal("fail to enqueue:", err)
	}

	time.Sleep(testJobTimeUnit / 2)

	// enqueue 2nd task:
	//  - 1st task is running
	//  - queue is empty
	err = runner.EnqueueTask("t", func(err error) {
		count++
		if count != 2 {
			t.Fatalf("callback count should be %d, actual %d", 2, count)
		}
	})
	if err != nil {
		t.Fatal("fail to enqueue:", err)
	}

	// try to enqueue a task:
	//  - 1st task is running
	//  - queue is NOT empty
	err = runner.EnqueueTask("t", neverCall)
	if err != ErrorAlreadyEnqueued {
		t.Fatal("unexpected err", err)
	}

	time.Sleep(testJobTimeUnit)

	// 1st task has been completed and cooling down...

	if count != 1 {
		t.Fatalf("callback count should be %d, actual %d", 1, count)
	}

	// try to enqueue a task:
	//  - in cool down time
	//  - queue is NOT empty
	err = runner.EnqueueTask("t", neverCall)
	if err != ErrorAlreadyEnqueued {
		t.Fatal("unexpected err", err)
	}

	time.Sleep(testJobTimeUnit)

	// 2nd task is running

	time.Sleep(testJobTimeUnit)

	// 2nd task has been completed and cooling down...

	if count != 2 {
		t.Fatalf("callback count should be %d, actual %d", 2, count)
	}

	// enqueue 3rd task:
	//  - in cool down time
	//  - queue is empty
	err = runner.EnqueueTask("t", func(err error) {
		count++
		if count != 3 {
			t.Fatalf("callback count should be %d, actual %d", 3, count)
		}
	})
	if err != nil {
		t.Fatal("fail to enqueue:", err)
	}

	time.Sleep(testJobTimeUnit)

	// 3rd task is running

	// enqueue 4th task:
	//  - third task is running
	//  - queue is empty
	err = runner.EnqueueTask("t", func(err error) {
		count++
		// disposed before running...
		if err != ErrorDisposed {
			t.Fatal("unexpected err", err)
		}
		if count != 4 {
			t.Fatalf("callback count should be %d, actual %d", 4, count)
		}
	})
	if err != nil {
		t.Fatal("fail to enqueue:", err)
	}

	// disposing:
	//  - 3rd task is running
	//  - queue is NOT empty
	if count != 2 {
		t.Fatalf("callback count should be %d, actual %d", 2, count)
	}
	runner.Dispose()
	if count != 4 {
		t.Fatalf("callback count should be %d, actual %d", 4, count)
	}

	d := time.Now().Sub(startAt)
	if d < testJobTimeUnit*5 || testJobTimeUnit*5+testJobTimeUnit/2 < d {
		t.Fatalf("unexpected elapse time: %d", d)
	}
}

func TestMultiTask(t *testing.T) {
	runner := New(2)
	defer runner.Dispose()

	runner.RegisterTask("t1", testJobTimeUnit, taskImpl)
	runner.RegisterTask("t2", testJobTimeUnit, taskImpl)
	runner.RegisterTask("t3", testJobTimeUnit, taskImpl)

	startAt := time.Now()

	wg := sync.WaitGroup{}
	wg.Add(5)

	callback := func(_ error) {
		wg.Done()
	}

	if err := runner.EnqueueTask("t1", callback); err != nil {
		t.Fatal("fail to enqueue:", err)
	}
	if err := runner.EnqueueTask("t2", callback); err != nil {
		t.Fatal("fail to enqueue:", err)
	}

	time.Sleep(testJobTimeUnit / 2)

	if err := runner.EnqueueTask("t1", callback); err != nil {
		t.Fatal("fail to enqueue:", err)
	}
	if err := runner.EnqueueTask("t2", callback); err != nil {
		t.Fatal("fail to enqueue:", err)
	}
	if err := runner.EnqueueTask("t3", callback); err != nil {
		t.Fatal("fail to enqueue:", err)
	}

	wg.Wait()

	d := time.Now().Sub(startAt)
	if d < testJobTimeUnit*3 || testJobTimeUnit*3+testJobTimeUnit/2 < d {
		t.Fatalf("unexpected elapse time: %d", d)
	}
}
