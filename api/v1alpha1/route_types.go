/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RouteBackend defines the target service for a routing rule.
type RouteBackend struct {
	// ServiceName is the target Kubernetes Service name.
	ServiceName string `json:"serviceName"`

	// ServicePort is the target Service port number.
	ServicePort int32 `json:"servicePort"`

	// Protocol is the protocol the backend expects. HTTPS triggers a DestinationRule.
	// +kubebuilder:validation:Enum=HTTP;HTTPS
	// +kubebuilder:default=HTTP
	// +optional
	Protocol string `json:"protocol,omitempty"`
}

// RouteRule defines a single routing rule mapping a host+path to a backend.
type RouteRule struct {
	// Host is the hostname to route (e.g., app.example.com).
	Host string `json:"host"`

	// Path is the path prefix to match.
	// +kubebuilder:default="/"
	// +optional
	Path string `json:"path,omitempty"`

	// Backend defines the target service.
	Backend RouteBackend `json:"backend"`
}

// RouteCORS defines the CORS policy applied to all rules.
type RouteCORS struct {
	// AllowOrigins is a list of regex patterns for allowed origins.
	// +optional
	AllowOrigins []string `json:"allowOrigins,omitempty"`

	// AllowMethods is a list of allowed HTTP methods.
	// +optional
	AllowMethods []string `json:"allowMethods,omitempty"`

	// AllowHeaders is a list of allowed request headers.
	// +optional
	AllowHeaders []string `json:"allowHeaders,omitempty"`

	// MaxAge is how long preflight results can be cached.
	// +kubebuilder:default="24h"
	// +optional
	MaxAge string `json:"maxAge,omitempty"`
}

// RouteGateway defines the Gateway reference.
type RouteGateway struct {
	// Name is the Gateway resource name.
	// +kubebuilder:default=istio-gateway
	// +optional
	Name string `json:"name,omitempty"`

	// Namespace is the Gateway resource namespace.
	// +kubebuilder:default=istio-system
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// RouteSpec defines the desired state of Route.
type RouteSpec struct {
	// Rules is a list of routing rules. Each rule maps a host+path to a backend service.
	// +kubebuilder:validation:MinItems=1
	Rules []RouteRule `json:"rules"`

	// SSLRedirect enables per-host HTTP→HTTPS redirect HTTPRoutes.
	// +kubebuilder:default=true
	// +optional
	SSLRedirect *bool `json:"sslRedirect,omitempty"`

	// Timeout is the request timeout (e.g., 60s, 300s). Applied to all rules.
	// +optional
	Timeout string `json:"timeout,omitempty"`

	// MaxBodySize is the max request body in bytes (e.g., 734003200 for 700MB). Applied to all rules.
	// +optional
	MaxBodySize string `json:"maxBodySize,omitempty"`

	// CORS is the CORS policy applied to all rules. Omit to disable CORS.
	// +optional
	CORS *RouteCORS `json:"cors,omitempty"`

	// Gateway is a custom gateway reference. Defaults to istio-gateway in istio-system.
	// +optional
	Gateway *RouteGateway `json:"gateway,omitempty"`
}

// RouteConditionType defines the condition types for Route status.
type RouteConditionType string

const (
	// RouteConditionReady indicates whether the Route is fully reconciled.
	RouteConditionReady RouteConditionType = "Ready"
	// RouteConditionSynced indicates whether child resources are in sync.
	RouteConditionSynced RouteConditionType = "Synced"
)

// RouteStatus defines the observed state of Route.
type RouteStatus struct {
	// Conditions represent the latest available observations of the Route's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ManagedResources is the count of resources managed by this Route.
	// +optional
	ManagedResources int `json:"managedResources,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Synced",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Route is the Schema for the routes API.
type Route struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RouteSpec   `json:"spec,omitempty"`
	Status RouteStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RouteList contains a list of Route.
type RouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Route `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Route{}, &RouteList{})
}
