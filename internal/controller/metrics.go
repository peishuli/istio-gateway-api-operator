package controller

import (
	platformv1alpha1 "github.com/istio-gateway-operator/route-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	routeReconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "route_reconcile_total",
			Help: "Total number of Route reconcile attempts grouped by result.",
		},
		[]string{"result"},
	)

	routeStatusPhase = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "route_status_phase",
			Help: "Route phase as one-hot gauge per namespace/route/phase.",
		},
		[]string{"namespace", "route", "phase"},
	)

	routeBackendHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "route_backend_health",
			Help: "Route backend health state as one-hot gauge per namespace/route/state.",
		},
		[]string{"namespace", "route", "state"},
	)

	routeManagedResources = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "route_managed_resources",
			Help: "Number of managed resources currently associated with a Route.",
		},
		[]string{"namespace", "route"},
	)
)

var phaseValues = []platformv1alpha1.RoutePhase{
	platformv1alpha1.RoutePhasePending,
	platformv1alpha1.RoutePhaseProvisioning,
	platformv1alpha1.RoutePhaseActive,
	platformv1alpha1.RoutePhaseDegraded,
}

var backendStates = []string{
	"healthy",
	"missing_service",
	"zero_endpoints",
	"conflict",
	"unknown",
}

func init() {
	ctrlmetrics.Registry.MustRegister(routeReconcileTotal)
	ctrlmetrics.Registry.MustRegister(routeStatusPhase)
	ctrlmetrics.Registry.MustRegister(routeBackendHealth)
	ctrlmetrics.Registry.MustRegister(routeManagedResources)
}

func setRoutePhaseMetric(route *platformv1alpha1.Route, current platformv1alpha1.RoutePhase) {
	for _, phase := range phaseValues {
		value := 0.0
		if phase == current {
			value = 1.0
		}
		routeStatusPhase.WithLabelValues(route.Namespace, route.Name, string(phase)).Set(value)
	}
}

func setRouteBackendHealthState(route *platformv1alpha1.Route, state string) {
	for _, s := range backendStates {
		value := 0.0
		if s == state {
			value = 1.0
		}
		routeBackendHealth.WithLabelValues(route.Namespace, route.Name, s).Set(value)
	}
}

func setManagedResourcesMetric(route *platformv1alpha1.Route, count int) {
	routeManagedResources.WithLabelValues(route.Namespace, route.Name).Set(float64(count))
}

func clearRouteMetrics(route *platformv1alpha1.Route) {
	for _, phase := range phaseValues {
		routeStatusPhase.DeleteLabelValues(route.Namespace, route.Name, string(phase))
	}
	for _, state := range backendStates {
		routeBackendHealth.DeleteLabelValues(route.Namespace, route.Name, state)
	}
	routeManagedResources.DeleteLabelValues(route.Namespace, route.Name)
}
