package controller

import (
	"testing"
	"time"

	platformv1alpha1 "github.com/istio-gateway-api-operator/route-operator/api/v1alpha1"
)

func TestBuildIstioHTTPRetryNil(t *testing.T) {
	if got := buildIstioHTTPRetry(nil); got != nil {
		t.Fatalf("expected nil retry policy, got %#v", got)
	}
}

func TestBuildIstioHTTPRetryWithFields(t *testing.T) {
	spec := &platformv1alpha1.RouteRetries{
		Attempts:      3,
		PerTryTimeout: "2s",
		RetryOn:       "gateway-error,connect-failure,refused-stream",
	}

	got := buildIstioHTTPRetry(spec)
	if got == nil {
		t.Fatal("expected retry policy, got nil")
	}
	if got.Attempts != 3 {
		t.Fatalf("expected attempts=3, got %d", got.Attempts)
	}
	if got.RetryOn != spec.RetryOn {
		t.Fatalf("expected retryOn=%q, got %q", spec.RetryOn, got.RetryOn)
	}
	if got.PerTryTimeout == nil {
		t.Fatal("expected perTryTimeout to be set")
	}
	if got.PerTryTimeout.AsDuration() != 2*time.Second {
		t.Fatalf("expected perTryTimeout=2s, got %s", got.PerTryTimeout.AsDuration())
	}
}

func TestBuildIstioHTTPRetryInvalidDuration(t *testing.T) {
	spec := &platformv1alpha1.RouteRetries{
		Attempts:      2,
		PerTryTimeout: "bad-duration",
	}

	got := buildIstioHTTPRetry(spec)
	if got == nil {
		t.Fatal("expected retry policy, got nil")
	}
	if got.PerTryTimeout != nil {
		t.Fatalf("expected perTryTimeout to be nil for invalid duration, got %#v", got.PerTryTimeout)
	}
}
