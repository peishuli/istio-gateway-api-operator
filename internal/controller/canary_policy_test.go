package controller

import (
	"testing"

	platformv1alpha1 "github.com/istio-gateway-operator/route-operator/api/v1alpha1"
)

func TestBuildIstioRouteDestinationsWithoutCanary(t *testing.T) {
	dests := buildIstioRouteDestinations("demo", platformv1alpha1.RouteBackend{ServiceName: "stable", ServicePort: 8080}, nil)
	if len(dests) != 1 {
		t.Fatalf("expected 1 destination, got %d", len(dests))
	}
	if dests[0].Weight != 0 {
		t.Fatalf("expected primary weight 0 when no canary, got %d", dests[0].Weight)
	}
	if got := dests[0].Destination.Host; got != "stable.demo.svc.cluster.local" {
		t.Fatalf("unexpected stable host %q", got)
	}
}

func TestBuildIstioRouteDestinationsWithCanary(t *testing.T) {
	dests := buildIstioRouteDestinations("demo", platformv1alpha1.RouteBackend{ServiceName: "stable", ServicePort: 8080}, &platformv1alpha1.RouteCanary{
		Backend: platformv1alpha1.RouteBackend{ServiceName: "canary", ServicePort: 8081},
		Weight:  20,
	})
	if len(dests) != 2 {
		t.Fatalf("expected 2 destinations, got %d", len(dests))
	}
	if dests[0].Weight != 80 {
		t.Fatalf("expected stable weight 80, got %d", dests[0].Weight)
	}
	if got := dests[1].Destination.Host; got != "canary.demo.svc.cluster.local" {
		t.Fatalf("unexpected canary host %q", got)
	}
	if dests[1].Weight != 20 {
		t.Fatalf("expected canary weight 20, got %d", dests[1].Weight)
	}
}

func TestRouteRuleBackends(t *testing.T) {
	rule := platformv1alpha1.RouteRule{Backend: platformv1alpha1.RouteBackend{ServiceName: "stable", ServicePort: 8080}}

	withoutCanary := routeRuleBackends(rule, nil)
	if len(withoutCanary) != 1 {
		t.Fatalf("expected 1 backend without canary, got %d", len(withoutCanary))
	}

	withCanary := routeRuleBackends(rule, &platformv1alpha1.RouteCanary{Backend: platformv1alpha1.RouteBackend{ServiceName: "canary", ServicePort: 8081}, Weight: 10})
	if len(withCanary) != 2 {
		t.Fatalf("expected 2 backends with canary, got %d", len(withCanary))
	}
	if withCanary[1].ServiceName != "canary" {
		t.Fatalf("unexpected canary backend %q", withCanary[1].ServiceName)
	}
}
