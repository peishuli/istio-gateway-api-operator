package controller

import (
	"testing"
)

func TestRateLimitFillInterval(t *testing.T) {
	tests := []struct {
		name    string
		unit    string
		want    string
		wantErr bool
	}{
		{name: "second", unit: "Second", want: "1s"},
		{name: "minute", unit: "Minute", want: "60s"},
		{name: "hour", unit: "Hour", want: "3600s"},
		{name: "invalid", unit: "Day", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := rateLimitFillInterval(tt.unit)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for unit %q", tt.unit)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("rateLimitFillInterval(%q)=%q, want %q", tt.unit, got, tt.want)
			}
		})
	}
}

func TestBuildRateLimitRoutePatch(t *testing.T) {
	patch := buildRateLimitRoutePatch("demo", "api.example.com", 20, 10, "60s")
	if patch == nil {
		t.Fatal("expected patch")
	}
	if patch.Match == nil || patch.Match.GetRouteConfiguration() == nil {
		t.Fatalf("unexpected patch match: %#v", patch.Match)
	}
	if got := patch.Match.GetRouteConfiguration().Vhost.Name; got != "api.example.com:443" {
		t.Fatalf("unexpected vhost name %q", got)
	}
}
