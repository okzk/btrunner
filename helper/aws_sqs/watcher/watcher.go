package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/okzk/btrunner"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Watch provides task enqueuing interface via Amazon SQS
func Watch(ctx context.Context, svc *sqs.SQS, url string, runner *btrunner.BatchingTaskRunner) error {
	for {
		log.Println("[DEBUG] receiving messages...")
		res, err := svc.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64(20),
		})
		if err != nil {
			if e, ok := err.(awserr.Error); ok {
				if e.Code() == "RequestCanceled" {
					return nil
				}
			}
			return err
		}
		for _, m := range res.Messages {
			go processMessage(svc, url, m, runner)
		}
	}
}

func processMessage(svc *sqs.SQS, url string, m *sqs.Message, runner *btrunner.BatchingTaskRunner) {
	messageID := aws.StringValue(m.MessageId)
	name, err := extractTaskName(m)
	if err != nil {
		log.Printf("[ERROR] [%s][unknown] failed to extract task name. err: %v", messageID, err)

		// the message is not retryable.
		deleteMessage(svc, url, m, messageID, "unknown")
		return
	}

	done := make(chan struct{}, 1)
	deleting := false
	mutex := sync.Mutex{}
	callback := func(err error) {
		mutex.Lock()
		deleting = true
		mutex.Unlock()

		if err != nil {
			log.Printf("[ERROR] [%s][%s] task failed. err: %v", messageID, name, err)
		} else {
			deleteMessage(svc, url, m, messageID, name)
		}
		close(done)
	}

	err = runner.EnqueueTask(name, callback)
	if err != nil {
		if err == btrunner.ErrorAlreadyEnqueued {
			log.Printf("[INFO] [%s][%s] task is batched", messageID, name)
			deleteMessage(svc, url, m, messageID, name)
		} else {
			log.Printf("[WARN] [%s][%s] fail to enqueue. err: %v", messageID, name, err)
		}
		return
	}
	log.Printf("[INFO] [%s][%s] task is enqueued", messageID, name)

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mutex.Lock()
			if !deleting {
				extendVisibilityTimeout(svc, url, m, messageID, name)
			}
			mutex.Unlock()
		case <-done:
			return
		}
	}
}

func deleteMessage(svc *sqs.SQS, url string, m *sqs.Message, messageID, name string) {
	log.Printf("[INFO] [%s][%s] deleting a sqs message", messageID, name)

	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		log.Printf("[WARN] [%s][%s] sqs.DeleteMessage returns error. err: %v", messageID, name, err)
	}
}

func extendVisibilityTimeout(svc *sqs.SQS, url string, m *sqs.Message, messageID, name string) {
	log.Printf("[DEBUG] [%s][%s] extending visibility timeout", messageID, name)
	_, err := svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(url),
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: aws.Int64(30),
	})
	if err != nil {
		log.Printf("[WARN] [%s][%s] sqs.ChangeMessageVisibility returns error. err: %v", messageID, name, err)
	}
}

// WatchUntilSignaled is a wrapper method of Watch method, and will be interrupted
// by SIGHUP, SIGINT, SIGTERM and SIGQUIT.
func WatchUntilSignaled(svc *sqs.SQS, url string, runner *btrunner.BatchingTaskRunner) error {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		<-sig
		cancel()
	}()

	return Watch(ctx, svc, url, runner)
}

func extractTaskName(m *sqs.Message) (string, error) {
	body := aws.StringValue(m.Body)

	var v interface{}
	err := json.Unmarshal([]byte(body), &v)
	if err != nil {
		// body is not a json. so consider it as a raw task name.
		return body, nil
	}
	switch vv := v.(type) {
	case string:
		// simple json string.
		return vv, nil
	case map[string]interface{}:
		// Consider body as an SNS message, and use it's `Message` as a task name.
		if msg, ok := vv["Message"].(string); ok {
			return msg, nil
		}
	}

	return "", errors.New("fail to parse body")
}
