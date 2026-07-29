package transform

import (
	"context"
	"errors"
	"testing"
)

func TestJSONPassthroughAcceptsJSON(t *testing.T) {
	payload := []byte(`{"driver":"boxia","samples":[1,2,3]}`)

	got, err := (JSONPassthrough{}).Transform(context.Background(), payload)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Transform() = %s, want %s", got, payload)
	}
}

func TestJSONPassthroughRejectsInvalidJSON(t *testing.T) {
	_, err := (JSONPassthrough{}).Transform(context.Background(), []byte(`{"broken":`))
	if !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("Transform() error = %v, want ErrInvalidJSON", err)
	}
}
