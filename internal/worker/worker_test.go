package worker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/transform"
)

func TestWorkerPublishesValidMessageAndCommits(t *testing.T) {
	source := testMessage([]byte(`{"speed":120}`))
	consumer := &fakeConsumer{batches: [][]Message{{source}}}
	producer := &fakeProducer{}

	runWorker(t, context.Background(), consumer, producer)

	if got, want := len(producer.messages), 1; got != want {
		t.Fatalf("produced messages = %d, want %d", got, want)
	}
	produced := producer.messages[0]
	if got, want := produced.Topic, "telemetry.json"; got != want {
		t.Fatalf("produced topic = %q, want %q", got, want)
	}
	if !bytes.Equal(produced.Value, source.Value) {
		t.Fatalf("produced value = %s, want %s", produced.Value, source.Value)
	}
	if !bytes.Equal(produced.Key, source.Key) {
		t.Fatalf("produced key = %q, want %q", produced.Key, source.Key)
	}
	if !produced.Timestamp.Equal(source.Timestamp) {
		t.Fatalf("produced timestamp = %v, want %v", produced.Timestamp, source.Timestamp)
	}
	if got, want := produced.Headers[0].Key, source.Headers[0].Key; got != want {
		t.Fatalf("header key = %q, want %q", got, want)
	}
	if !bytes.Equal(produced.Headers[0].Value, source.Headers[0].Value) {
		t.Fatalf("header value = %q, want %q", produced.Headers[0].Value, source.Headers[0].Value)
	}
	if got, want := len(consumer.committed), 1; got != want {
		t.Fatalf("committed messages = %d, want %d", got, want)
	}
}

func TestWorkerRoutesInvalidJSONToDLQAndCommits(t *testing.T) {
	source := testMessage([]byte(`{"broken":`))
	consumer := &fakeConsumer{batches: [][]Message{{source}}}
	producer := &fakeProducer{}

	runWorker(t, context.Background(), consumer, producer)

	if got, want := len(producer.messages), 1; got != want {
		t.Fatalf("produced messages = %d, want %d", got, want)
	}
	produced := producer.messages[0]
	if got, want := produced.Topic, "telemetry.dlq"; got != want {
		t.Fatalf("produced topic = %q, want %q", got, want)
	}

	var record dlqRecord
	if err := json.Unmarshal(produced.Value, &record); err != nil {
		t.Fatalf("unmarshal DLQ record: %v", err)
	}
	if got, want := record.PayloadBase64, base64.StdEncoding.EncodeToString(source.Value); got != want {
		t.Fatalf("payload_base64 = %q, want %q", got, want)
	}
	if got, want := record.ErrorType, "transform_error"; got != want {
		t.Fatalf("error_type = %q, want %q", got, want)
	}
	if got, want := len(consumer.committed), 1; got != want {
		t.Fatalf("committed messages = %d, want %d", got, want)
	}
}

func TestWorkerDoesNotCommitWhenPublishingFails(t *testing.T) {
	source := testMessage([]byte(`{"speed":120}`))
	consumer := &fakeConsumer{batches: [][]Message{{source}}}
	ctx, cancel := context.WithCancel(context.Background())
	producer := &fakeProducer{
		err: errors.New("broker unavailable"),
		onProduce: func() {
			cancel()
		},
	}

	runWorker(t, ctx, consumer, producer)

	if got := len(consumer.committed); got != 0 {
		t.Fatalf("committed messages = %d, want 0", got)
	}
}

func TestWorkerStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &fakeConsumer{block: true}
	producer := &fakeProducer{}

	done := make(chan error, 1)
	go func() {
		done <- newTestWorker(consumer, producer).Run(ctx)
	}()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
}

func runWorker(t *testing.T, ctx context.Context, consumer *fakeConsumer, producer *fakeProducer) {
	t.Helper()
	if err := newTestWorker(consumer, producer).Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func newTestWorker(consumer Consumer, producer Producer) *Worker {
	return New(consumer, producer, transform.JSONPassthrough{}, Options{
		OutputTopic:  "telemetry.json",
		DLQTopic:     "telemetry.dlq",
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		RetryInitial: time.Millisecond,
		RetryMax:     time.Millisecond,
	})
}

func testMessage(value []byte) Message {
	return Message{
		Topic:     "telemetry.raw",
		Partition: 2,
		Offset:    41,
		Timestamp: time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
		Key:       []byte("car-7"),
		Value:     value,
		Headers:   []Header{{Key: "trace-id", Value: []byte("abc")}},
	}
}

type fakeConsumer struct {
	mu        sync.Mutex
	batches   [][]Message
	committed []Message
	block     bool
}

func (f *fakeConsumer) Poll(ctx context.Context) ([]Message, error) {
	f.mu.Lock()
	if len(f.batches) > 0 {
		batch := f.batches[0]
		f.batches = f.batches[1:]
		f.mu.Unlock()
		return batch, nil
	}
	block := f.block
	f.mu.Unlock()

	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, context.Canceled
}

func (f *fakeConsumer) Commit(_ context.Context, message Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, message)
	return nil
}

type fakeProducer struct {
	mu        sync.Mutex
	messages  []Message
	err       error
	onProduce func()
}

func (f *fakeProducer) Produce(_ context.Context, message Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onProduce != nil {
		f.onProduce()
	}
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, message)
	return nil
}
