package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

const (
	SASLNone        = "none"
	SASLPlain       = "plain"
	SASLSCRAMSHA256 = "scram-sha-256"
	SASLSCRAMSHA512 = "scram-sha-512"
)

type Config struct {
	Kafka    Kafka
	LogLevel slog.Level
}

type Kafka struct {
	Brokers        []string
	InputTopic     string
	OutputTopic    string
	DLQTopic       string
	ConsumerGroup  string
	ClientID       string
	TLSEnabled     bool
	SASLMechanism  string
	SASLUsername   string
	SASLPassword   string
	OffsetReset    string
	MaxPollRecords int
}

func Load() (Config, error) {
	var cfg Config

	cfg.Kafka.Brokers = splitCSV(os.Getenv("KAFKA_BROKERS"))
	cfg.Kafka.InputTopic = strings.TrimSpace(os.Getenv("KAFKA_INPUT_TOPIC"))
	cfg.Kafka.OutputTopic = strings.TrimSpace(os.Getenv("KAFKA_OUTPUT_TOPIC"))
	cfg.Kafka.DLQTopic = strings.TrimSpace(os.Getenv("KAFKA_DLQ_TOPIC"))
	cfg.Kafka.ConsumerGroup = strings.TrimSpace(os.Getenv("KAFKA_CONSUMER_GROUP"))
	cfg.Kafka.ClientID = envOrDefault("KAFKA_CLIENT_ID", "boxiaracing-telemetry-worker")
	cfg.Kafka.SASLMechanism = strings.ToLower(envOrDefault("KAFKA_SASL_MECHANISM", SASLNone))
	cfg.Kafka.SASLUsername = os.Getenv("KAFKA_SASL_USERNAME")
	cfg.Kafka.SASLPassword = os.Getenv("KAFKA_SASL_PASSWORD")
	cfg.Kafka.OffsetReset = strings.ToLower(envOrDefault("KAFKA_OFFSET_RESET", "earliest"))

	var err error
	cfg.Kafka.TLSEnabled, err = boolEnv("KAFKA_TLS_ENABLED", true)
	if err != nil {
		return Config{}, err
	}

	cfg.Kafka.MaxPollRecords, err = intEnv("KAFKA_MAX_POLL_RECORDS", 100)
	if err != nil {
		return Config{}, err
	}

	cfg.LogLevel, err = parseLogLevel(envOrDefault("LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}

	if err := cfg.Kafka.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (cfg Kafka) Validate() error {
	required := map[string]bool{
		"KAFKA_BROKERS":        len(cfg.Brokers) > 0,
		"KAFKA_INPUT_TOPIC":    cfg.InputTopic != "",
		"KAFKA_OUTPUT_TOPIC":   cfg.OutputTopic != "",
		"KAFKA_DLQ_TOPIC":      cfg.DLQTopic != "",
		"KAFKA_CONSUMER_GROUP": cfg.ConsumerGroup != "",
	}
	for name, present := range required {
		if !present {
			return fmt.Errorf("%s is required", name)
		}
	}

	switch cfg.SASLMechanism {
	case SASLNone:
	case SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512:
		if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
			return fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for %s", cfg.SASLMechanism)
		}
	default:
		return fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", cfg.SASLMechanism)
	}

	if cfg.OffsetReset != "earliest" && cfg.OffsetReset != "latest" {
		return fmt.Errorf("KAFKA_OFFSET_RESET must be earliest or latest")
	}
	if cfg.MaxPollRecords < 1 {
		return fmt.Errorf("KAFKA_MAX_POLL_RECORDS must be greater than zero")
	}

	return nil
}

func splitCSV(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func boolEnv(name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return parsed, nil
}

func intEnv(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return parsed, nil
}

func parseLogLevel(value string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(value))); err != nil {
		return 0, fmt.Errorf("invalid LOG_LEVEL %q: %w", value, err)
	}
	return level, nil
}
