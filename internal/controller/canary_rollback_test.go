package controller

import (
	"testing"

	platformv1alpha1 "github.com/istio-gateway-api-operator/route-operator/api/v1alpha1"
)

func TestNormalizeCanaryProbePath(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: "/"},
		{name: "already prefixed", in: "/healthz", want: "/healthz"},
		{name: "without slash", in: "healthz", want: "/healthz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeCanaryProbePath(tt.in); got != tt.want {
				t.Fatalf("normalizeCanaryProbePath(%q)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestCanaryBackendProbeURL(t *testing.T) {
	backend := platformv1alpha1.RouteBackend{ServiceName: "smoke-canary-svc", ServicePort: 8081}
	got := canaryBackendProbeURL("route-operator-smoke", backend, "healthz")
	want := "http://smoke-canary-svc.route-operator-smoke.svc.cluster.local:8081/healthz"
	if got != want {
		t.Fatalf("canaryBackendProbeURL()=%q, want %q", got, want)
	}
}
