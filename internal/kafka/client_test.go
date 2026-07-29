package kafka

import (
	"testing"

	"github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/config"
)

func TestSASLMechanisms(t *testing.T) {
	t.Parallel()

	for _, mechanism := range []string{
		config.SASLPlain,
		config.SASLSCRAMSHA256,
		config.SASLSCRAMSHA512,
	} {
		mechanism := mechanism
		t.Run(mechanism, func(t *testing.T) {
			t.Parallel()
			got, err := saslMechanism(config.Kafka{
				SASLMechanism: mechanism,
				SASLUsername:  "user",
				SASLPassword:  "password",
			})
			if err != nil {
				t.Fatalf("saslMechanism() error = %v", err)
			}
			if got == nil {
				t.Fatal("saslMechanism() = nil, want mechanism")
			}
		})
	}
}

func TestSASLDisabled(t *testing.T) {
	t.Parallel()
	got, err := saslMechanism(config.Kafka{SASLMechanism: config.SASLNone})
	if err != nil {
		t.Fatalf("saslMechanism() error = %v", err)
	}
	if got != nil {
		t.Fatalf("saslMechanism() = %T, want nil", got)
	}
}
