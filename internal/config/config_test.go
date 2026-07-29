package config

import (
	"log/slog"
	"strings"
	"testing"
)

func TestLoadValidConfiguration(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("KAFKA_BROKERS", "broker-1:9092, broker-2:9092")
	t.Setenv("KAFKA_TLS_ENABLED", "false")
	t.Setenv("KAFKA_MAX_POLL_RECORDS", "42")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if got, want := len(cfg.Kafka.Brokers), 2; got != want {
		t.Fatalf("len(Brokers) = %d, want %d", got, want)
	}
	if cfg.Kafka.TLSEnabled {
		t.Fatal("TLSEnabled = true, want false")
	}
	if got, want := cfg.Kafka.MaxPollRecords, 42; got != want {
		t.Fatalf("MaxPollRecords = %d, want %d", got, want)
	}
	if got, want := cfg.LogLevel, slog.LevelDebug; got != want {
		t.Fatalf("LogLevel = %v, want %v", got, want)
	}
}

func TestLoadRequiresCoreKafkaSettings(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("KAFKA_INPUT_TOPIC", "")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "KAFKA_INPUT_TOPIC is required") {
		t.Fatalf("Load() error = %v, want missing input topic error", err)
	}
}

func TestLoadValidatesSASLCredentials(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("KAFKA_SASL_MECHANISM", SASLSCRAMSHA256)
	t.Setenv("KAFKA_SASL_USERNAME", "")
	t.Setenv("KAFKA_SASL_PASSWORD", "")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "KAFKA_SASL_USERNAME") {
		t.Fatalf("Load() error = %v, want missing SASL credentials error", err)
	}
}

func TestLoadRejectsUnknownSASLMechanism(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("KAFKA_SASL_MECHANISM", "magic")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "unsupported KAFKA_SASL_MECHANISM") {
		t.Fatalf("Load() error = %v, want unsupported mechanism error", err)
	}
}

func setValidEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("KAFKA_INPUT_TOPIC", "telemetry.raw")
	t.Setenv("KAFKA_OUTPUT_TOPIC", "telemetry.json")
	t.Setenv("KAFKA_DLQ_TOPIC", "telemetry.dlq")
	t.Setenv("KAFKA_CONSUMER_GROUP", "telemetry-worker")
	t.Setenv("KAFKA_CLIENT_ID", "")
	t.Setenv("KAFKA_TLS_ENABLED", "")
	t.Setenv("KAFKA_SASL_MECHANISM", SASLNone)
	t.Setenv("KAFKA_SASL_USERNAME", "")
	t.Setenv("KAFKA_SASL_PASSWORD", "")
	t.Setenv("KAFKA_OFFSET_RESET", "")
	t.Setenv("KAFKA_MAX_POLL_RECORDS", "")
	t.Setenv("LOG_LEVEL", "")
}
