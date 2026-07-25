package v1alpha1

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestRouteDefaulterDefaultsGatewayRef(t *testing.T) {
	route := &Route{}
	d := &routeDefaulter{}
	if err := d.Default(context.Background(), route); err != nil {
		t.Fatalf("default failed: %v", err)
	}
	if route.Spec.Gateway == nil {
		t.Fatal("expected gateway to be defaulted")
	}
	if route.Spec.Gateway.Name != defaultGatewayName {
		t.Fatalf("expected default gateway name %q, got %q", defaultGatewayName, route.Spec.Gateway.Name)
	}
	if route.Spec.Gateway.Namespace != defaultGatewayNamespace {
		t.Fatalf("expected default gateway namespace %q, got %q", defaultGatewayNamespace, route.Spec.Gateway.Namespace)
	}
}

func TestHostMatchesPattern(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		pattern string
		want    bool
	}{
		{name: "exact match", host: "api.example.com", pattern: "api.example.com", want: true},
		{name: "exact mismatch", host: "api.example.com", pattern: "web.example.com", want: false},
		{name: "wildcard match", host: "api.example.com", pattern: "*.example.com", want: true},
		{name: "wildcard apex mismatch", host: "example.com", pattern: "*.example.com", want: false},
		{name: "wildcard suffix mismatch", host: "api.other.com", pattern: "*.example.com", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hostMatchesPattern(tt.host, tt.pattern); got != tt.want {
				t.Fatalf("hostMatchesPattern(%q,%q)=%v, want %v", tt.host, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestValidateGatewayHostnames(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("add route scheme: %v", err)
	}
	if err := gatewayv1.Install(scheme); err != nil {
		t.Fatalf("add gateway scheme: %v", err)
	}

	gwHost := gatewayv1.Hostname("*.example.com")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "istio-gateway", Namespace: "istio-system"},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{{
				Name:     gatewayv1.SectionName("https"),
				Port:     gatewayv1.PortNumber(443),
				Protocol: gatewayv1.HTTPSProtocolType,
				Hostname: &gwHost,
			}},
		},
	}

	routeWebhookClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(gw).Build()

	good := &Route{
		ObjectMeta: metav1.ObjectMeta{Name: "good", Namespace: "demo"},
		Spec: RouteSpec{Rules: []RouteRule{{Host: "api.example.com", Backend: RouteBackend{ServiceName: "svc", ServicePort: 8080}}}},
	}
	if err := validateGatewayHostnames(context.Background(), good); err != nil {
		t.Fatalf("expected good host to pass, got error: %v", err)
	}

	bad := &Route{
		ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "demo"},
		Spec: RouteSpec{Rules: []RouteRule{{Host: "api.other.com", Backend: RouteBackend{ServiceName: "svc", ServicePort: 8080}}}},
	}
	if err := validateGatewayHostnames(context.Background(), bad); err == nil {
		t.Fatal("expected invalid host to be rejected")
	}
}

func TestValidateRouteConflicts(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("add route scheme: %v", err)
	}

	existing := &Route{
		ObjectMeta: metav1.ObjectMeta{Name: "existing", Namespace: "demo"},
		Spec: RouteSpec{
			Gateway: &RouteGateway{Name: "istio-gateway", Namespace: "istio-system"},
			Rules: []RouteRule{{Host: "api.example.com", Path: "/", Backend: RouteBackend{ServiceName: "svc-a", ServicePort: 8080}}},
		},
	}

	routeWebhookClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	candidate := &Route{
		ObjectMeta: metav1.ObjectMeta{Name: "candidate", Namespace: "demo"},
		Spec: RouteSpec{
			Gateway: &RouteGateway{Name: "istio-gateway", Namespace: "istio-system"},
			Rules: []RouteRule{{Host: "api.example.com", Path: "/", Backend: RouteBackend{ServiceName: "svc-b", ServicePort: 8080}}},
		},
	}
	if err := validateRouteConflicts(context.Background(), candidate); err == nil {
		t.Fatal("expected conflict to be rejected")
	}

	nonConflict := &Route{
		ObjectMeta: metav1.ObjectMeta{Name: "candidate-ok", Namespace: "demo"},
		Spec: RouteSpec{
			Gateway: &RouteGateway{Name: "istio-gateway", Namespace: "istio-system"},
			Rules: []RouteRule{{Host: "web.example.com", Path: "/", Backend: RouteBackend{ServiceName: "svc-b", ServicePort: 8080}}},
		},
	}
	if err := validateRouteConflicts(context.Background(), nonConflict); err != nil {
		t.Fatalf("expected non-conflicting route to pass, got: %v", err)
	}
}
