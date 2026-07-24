package controller

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// wrapperspb_BoolValue is a convenience wrapper.
type wrapperspb_BoolValue = wrapperspb.BoolValue

// parseDuration parses a Kubernetes-style duration string (e.g., "60s", "300s", "24h")
// into a protobuf Duration.
func parseDuration(s string) (*durationpb.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return nil, err
	}
	return durationpb.New(d), nil
}

// mustStruct converts a map to a protobuf Struct, panicking on error.
func mustStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}
