package watcher

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/okzk/btrunner"
	"log"
	"os"
	"os/signal"
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
	name := aws.StringValue(m.Body)
	messageID := aws.StringValue(m.MessageId)

	done := make(chan struct{}, 1)
	callback := func(err error) {
		if err != nil {
			log.Printf("[ERROR] [%s][%s] task failed. err: %v", messageID, name, err)
		} else {
			deleteMessage(svc, url, m)
		}
		close(done)
	}

	err := runner.EnqueueTask(name, callback)
	if err != nil {
		if err == btrunner.ErrorAlreadyEnqueued {
			log.Printf("[INFO] [%s][%s] task is batched", messageID, name)
			deleteMessage(svc, url, m)
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
			extendVisibilityTimeout(svc, url, m)
		case <-done:
			return
		}
	}
}

func deleteMessage(svc *sqs.SQS, url string, m *sqs.Message) {
	name := aws.StringValue(m.Body)
	messageID := aws.StringValue(m.MessageId)

	log.Printf("[INFO] [%s][%s] deleting a sqs message", messageID, name)

	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		log.Printf("[WARN] [%s][%s] sqs.DeleteMessage returns error. err: %v", messageID, name, err)
	}
}

func extendVisibilityTimeout(svc *sqs.SQS, url string, m *sqs.Message) {
	name := aws.StringValue(m.Body)
	messageID := aws.StringValue(m.MessageId)

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
