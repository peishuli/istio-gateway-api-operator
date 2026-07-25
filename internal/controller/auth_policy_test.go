package controller

import (
	"testing"

	platformv1alpha1 "github.com/istio-gateway-operator/route-operator/api/v1alpha1"
)

func TestAuthzPathPattern(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: "/*"},
		{name: "root", in: "/", want: "/*"},
		{name: "prefix", in: "/api", want: "/api*"},
		{name: "trailing slash", in: "/api/", want: "/api/*"},
		{name: "already wildcard", in: "/api/*", want: "/api/*"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := authzPathPattern(tt.in); got != tt.want {
				t.Fatalf("authzPathPattern(%q)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestBuildAuthorizationPolicyRules(t *testing.T) {
	rules := []platformv1alpha1.RouteRule{
		{Host: "api.example.com", Path: "/api"},
		{Host: "web.example.com", Path: "/"},
	}

	got := buildAuthorizationPolicyRules(rules)
	if len(got) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(got))
	}

	if got[0].To[0].Operation.Hosts[0] != "api.example.com" {
		t.Fatalf("unexpected host in first rule: %#v", got[0].To[0].Operation.Hosts)
	}
	if got[0].To[0].Operation.Paths[0] != "/api*" {
		t.Fatalf("unexpected path in first rule: %#v", got[0].To[0].Operation.Paths)
	}
	if got[0].From[0].Source.RequestPrincipals[0] != "*" {
		t.Fatalf("unexpected principals in first rule: %#v", got[0].From[0].Source.RequestPrincipals)
	}

	if got[1].To[0].Operation.Hosts[0] != "web.example.com" {
		t.Fatalf("unexpected host in second rule: %#v", got[1].To[0].Operation.Hosts)
	}
	if got[1].To[0].Operation.Paths[0] != "/*" {
		t.Fatalf("unexpected path in second rule: %#v", got[1].To[0].Operation.Paths)
	}
}
