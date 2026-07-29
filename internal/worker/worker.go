package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/transform"
)

type Header struct {
	Key   string
	Value []byte
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
}

type Consumer interface {
	Poll(context.Context) ([]Message, error)
	Commit(context.Context, Message) error
}

type Producer interface {
	Produce(context.Context, Message) error
}

type Options struct {
	OutputTopic  string
	DLQTopic     string
	Logger       *slog.Logger
	RetryInitial time.Duration
	RetryMax     time.Duration
}

type Worker struct {
	consumer     Consumer
	producer     Producer
	transformer  transform.Transformer
	outputTopic  string
	dlqTopic     string
	logger       *slog.Logger
	retryInitial time.Duration
	retryMax     time.Duration
}

func New(consumer Consumer, producer Producer, transformer transform.Transformer, options Options) *Worker {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.RetryInitial <= 0 {
		options.RetryInitial = 250 * time.Millisecond
	}
	if options.RetryMax <= 0 {
		options.RetryMax = 10 * time.Second
	}

	return &Worker{
		consumer:     consumer,
		producer:     producer,
		transformer:  transformer,
		outputTopic:  options.OutputTopic,
		dlqTopic:     options.DLQTopic,
		logger:       options.Logger,
		retryInitial: options.RetryInitial,
		retryMax:     options.RetryMax,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		messages, err := w.consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			w.logger.Error("kafka poll failed", "error", err)
			if err := wait(ctx, w.retryInitial); err != nil {
				return nil
			}
			continue
		}

		for _, message := range messages {
			if ctx.Err() != nil {
				return nil
			}
			if err := w.process(ctx, message); err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
		}
	}
}

func (w *Worker) process(ctx context.Context, source Message) error {
	value, transformErr := w.transformer.Transform(ctx, source.Value)

	target := cloneForTopic(source, w.outputTopic, value)
	if transformErr != nil {
		dlqValue, err := json.Marshal(newDLQRecord(source, transformErr))
		if err != nil {
			return fmt.Errorf("marshal DLQ record: %w", err)
		}
		target = cloneForTopic(source, w.dlqTopic, dlqValue)
		w.logger.Warn(
			"message transformation failed; routing to DLQ",
			"error", transformErr,
			"topic", source.Topic,
			"partition", source.Partition,
			"offset", source.Offset,
		)
	}

	if err := w.retry(ctx, "publish kafka message", func() error {
		return w.producer.Produce(ctx, target)
	}); err != nil {
		return err
	}

	if err := w.retry(ctx, "commit kafka offset", func() error {
		return w.consumer.Commit(ctx, source)
	}); err != nil {
		return err
	}

	return nil
}

func (w *Worker) retry(ctx context.Context, operation string, fn func() error) error {
	delay := w.retryInitial
	for {
		if err := fn(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.logger.Error(operation+" failed; retrying", "error", err, "retry_in", delay)
			if err := wait(ctx, delay); err != nil {
				return err
			}
			delay = min(delay*2, w.retryMax)
			continue
		}
		return nil
	}
}

func cloneForTopic(source Message, topic string, value []byte) Message {
	return Message{
		Topic:     topic,
		Timestamp: source.Timestamp,
		Key:       append([]byte(nil), source.Key...),
		Value:     append([]byte(nil), value...),
		Headers:   cloneHeaders(source.Headers),
	}
}

func cloneHeaders(headers []Header) []Header {
	cloned := make([]Header, len(headers))
	for i, header := range headers {
		cloned[i] = Header{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	}
	return cloned
}

type dlqRecord struct {
	SourceTopic     string    `json:"source_topic"`
	SourcePartition int32     `json:"source_partition"`
	SourceOffset    int64     `json:"source_offset"`
	SourceTimestamp time.Time `json:"source_timestamp"`
	KeyBase64       string    `json:"key_base64"`
	PayloadBase64   string    `json:"payload_base64"`
	ErrorType       string    `json:"error_type"`
	ErrorMessage    string    `json:"error_message"`
	FailedAt        time.Time `json:"failed_at"`
}

func newDLQRecord(source Message, err error) dlqRecord {
	return dlqRecord{
		SourceTopic:     source.Topic,
		SourcePartition: source.Partition,
		SourceOffset:    source.Offset,
		SourceTimestamp: source.Timestamp.UTC(),
		KeyBase64:       base64.StdEncoding.EncodeToString(source.Key),
		PayloadBase64:   base64.StdEncoding.EncodeToString(source.Value),
		ErrorType:       "transform_error",
		ErrorMessage:    err.Error(),
		FailedAt:        time.Now().UTC(),
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
