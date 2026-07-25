package controller

import (
	"reflect"
	"testing"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	platformv1alpha1 "github.com/istio-gateway-operator/route-operator/api/v1alpha1"
)

func TestBuildGatewayHTTPHeaderFiltersNil(t *testing.T) {
	if got := buildGatewayHTTPHeaderFilters(nil); got != nil {
		t.Fatalf("expected nil filters, got %#v", got)
	}
}

func TestBuildGatewayHTTPHeaderFiltersRequestAndResponse(t *testing.T) {
	headers := &platformv1alpha1.RouteHeaders{
		Request: &platformv1alpha1.RouteHeaderOperations{
			Set: map[string]string{"x-foo": "bar"},
			Add: map[string]string{"x-add": "v1"},
		},
		Response: &platformv1alpha1.RouteHeaderOperations{
			Remove: []string{"x-hide"},
		},
	}

	got := buildGatewayHTTPHeaderFilters(headers)
	if len(got) != 2 {
		t.Fatalf("expected 2 filters, got %d", len(got))
	}

	if got[0].Type != gatewayv1.HTTPRouteFilterRequestHeaderModifier {
		t.Fatalf("expected first filter type %q, got %q", gatewayv1.HTTPRouteFilterRequestHeaderModifier, got[0].Type)
	}
	if got[0].RequestHeaderModifier == nil || len(got[0].RequestHeaderModifier.Set) != 1 || len(got[0].RequestHeaderModifier.Add) != 1 {
		t.Fatalf("unexpected request header modifier: %#v", got[0].RequestHeaderModifier)
	}

	if got[1].Type != gatewayv1.HTTPRouteFilterResponseHeaderModifier {
		t.Fatalf("expected second filter type %q, got %q", gatewayv1.HTTPRouteFilterResponseHeaderModifier, got[1].Type)
	}
	if got[1].ResponseHeaderModifier == nil || !reflect.DeepEqual(got[1].ResponseHeaderModifier.Remove, []string{"x-hide"}) {
		t.Fatalf("unexpected response header modifier: %#v", got[1].ResponseHeaderModifier)
	}
}

func TestBuildIstioHeaders(t *testing.T) {
	headers := &platformv1alpha1.RouteHeaders{
		Request: &platformv1alpha1.RouteHeaderOperations{
			Set:    map[string]string{"x-request-id": "123"},
			Remove: []string{"x-drop"},
		},
		Response: &platformv1alpha1.RouteHeaderOperations{
			Add: map[string]string{"x-response-tag": "ok"},
		},
	}

	got := buildIstioHeaders(headers)
	if got == nil {
		t.Fatal("expected headers, got nil")
	}
	if got.Request == nil || got.Request.Set["x-request-id"] != "123" {
		t.Fatalf("unexpected request headers: %#v", got.Request)
	}
	if !reflect.DeepEqual(got.Request.Remove, []string{"x-drop"}) {
		t.Fatalf("unexpected request remove headers: %#v", got.Request.Remove)
	}
	if got.Response == nil || got.Response.Add["x-response-tag"] != "ok" {
		t.Fatalf("unexpected response headers: %#v", got.Response)
	}
}

func TestBuildIstioHeadersEmpty(t *testing.T) {
	headers := &platformv1alpha1.RouteHeaders{
		Request: &platformv1alpha1.RouteHeaderOperations{},
	}
	if got := buildIstioHeaders(headers); got != nil {
		t.Fatalf("expected nil headers for empty operations, got %#v", got)
	}
}
