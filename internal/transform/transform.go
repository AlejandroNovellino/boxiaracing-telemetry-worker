package transform

import (
	"context"
	"encoding/json"
	"errors"
)

var ErrInvalidJSON = errors.New("payload is not valid JSON")

// Transformer converts a raw Kafka value into the JSON document expected by
// the output topic.
type Transformer interface {
	Transform(context.Context, []byte) ([]byte, error)
}

// JSONPassthrough is the provisional transformer used while the binary
// telemetry schema is incorporated. It validates JSON but deliberately does
// not attempt to discover or decode Base64 fields.
type JSONPassthrough struct{}

func (JSONPassthrough) Transform(_ context.Context, payload []byte) ([]byte, error) {
	if !json.Valid(payload) {
		return nil, ErrInvalidJSON
	}

	return payload, nil
}
