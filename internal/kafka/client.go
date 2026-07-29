package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/config"
	"github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/worker"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Client struct {
	client      *kgo.Client
	mu          sync.Mutex
	pollStarted bool
	maxRecords  int
}

func New(cfg config.Kafka) (*Client, error) {
	options := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.InputTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	if cfg.OffsetReset == "latest" {
		options = append(options, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	} else {
		options = append(options, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	if cfg.TLSEnabled {
		options = append(options, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}

	mechanism, err := saslMechanism(cfg)
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		options = append(options, kgo.SASL(mechanism))
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, err
	}

	return &Client{client: client, maxRecords: cfg.MaxPollRecords}, nil
}

func (c *Client) Poll(ctx context.Context) ([]worker.Message, error) {
	c.mu.Lock()
	if c.pollStarted {
		c.client.AllowRebalance()
	}
	c.pollStarted = true
	c.mu.Unlock()

	fetches := c.client.PollRecords(ctx, c.maxRecords)
	if err := fetches.Err(); err != nil {
		return nil, err
	}

	messages := make([]worker.Message, 0, fetches.NumRecords())
	fetches.EachRecord(func(record *kgo.Record) {
		headers := make([]worker.Header, len(record.Headers))
		for i, header := range record.Headers {
			headers[i] = worker.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
		}
		messages = append(messages, worker.Message{
			Topic:     record.Topic,
			Partition: record.Partition,
			Offset:    record.Offset,
			Timestamp: record.Timestamp,
			Key:       append([]byte(nil), record.Key...),
			Value:     append([]byte(nil), record.Value...),
			Headers:   headers,
		})
	})

	return messages, nil
}

func (c *Client) Produce(ctx context.Context, message worker.Message) error {
	headers := make([]kgo.RecordHeader, len(message.Headers))
	for i, header := range message.Headers {
		headers[i] = kgo.RecordHeader{Key: header.Key, Value: header.Value}
	}

	result := c.client.ProduceSync(ctx, &kgo.Record{
		Topic:     message.Topic,
		Timestamp: message.Timestamp,
		Key:       message.Key,
		Value:     message.Value,
		Headers:   headers,
	})

	return result.FirstErr()
}

func (c *Client) Commit(ctx context.Context, message worker.Message) error {
	return c.client.CommitRecords(ctx, &kgo.Record{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
	})
}

func (c *Client) Close() {
	c.mu.Lock()
	if c.pollStarted {
		c.client.AllowRebalance()
	}
	c.mu.Unlock()
	c.client.Close()
}

func saslMechanism(cfg config.Kafka) (sasl.Mechanism, error) {
	switch cfg.SASLMechanism {
	case config.SASLNone:
		return nil, nil
	case config.SASLPlain:
		return plain.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsMechanism(), nil
	case config.SASLSCRAMSHA256:
		return scram.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsSha256Mechanism(), nil
	case config.SASLSCRAMSHA512:
		return scram.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", cfg.SASLMechanism)
	}
}
