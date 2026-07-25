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

package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	networkingv1 "istio.io/client-go/pkg/apis/networking/v1"
	istiov1alpha3 "istio.io/client-go/pkg/apis/networking/v1alpha3"
	istionetv1 "istio.io/api/networking/v1"
	istionetv1alpha3 "istio.io/api/networking/v1alpha3"
	istiosecv1beta1 "istio.io/api/security/v1beta1"
	istiotypev1beta1 "istio.io/api/type/v1beta1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	platformv1alpha1 "github.com/istio-gateway-operator/route-operator/api/v1alpha1"
	securityv1beta1 "istio.io/client-go/pkg/apis/security/v1beta1"
)

const (
	routeFinalizer = "istio-gateway-api-operator.io/finalizer"
	managedByLabel = "istio-gateway-api-operator.io/managed-by"
	routeNameLabel = "istio-gateway-api-operator.io/route-name"
	requeueDelay   = 15 * time.Second
	certWarnBefore = 30 * 24 * time.Hour
	defaultCanaryProbeInterval = 30 * time.Second
	defaultCanaryCooldown      = 5 * time.Minute
)

// RouteReconciler reconciles a Route object
type RouteReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	canaryStateMu        sync.Mutex
	canaryFailureCounts  map[string]int
	canaryRollbackUntil  map[string]time.Time
}

// +kubebuilder:rbac:groups=istio-gateway-api-operator.io,resources=routes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=istio-gateway-api-operator.io,resources=routes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=istio-gateway-api-operator.io,resources=routes/finalizers,verbs=update
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=virtualservices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=destinationrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=envoyfilters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=security.istio.io,resources=requestauthentications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=security.istio.io,resources=authorizationpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=endpoints,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update

func (r *RouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	outcome := "error"
	defer func() {
		routeReconcileTotal.WithLabelValues(outcome).Inc()
	}()

	// Fetch the Route instance
	route := &platformv1alpha1.Route{}
	if err := r.Get(ctx, req.NamespacedName, route); err != nil {
		if errors.IsNotFound(err) {
			outcome = "not_found"
			return ctrl.Result{}, nil
		}
		outcome = "get_error"
		return ctrl.Result{}, err
	}

	// Handle deletion
	if !route.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(route, routeFinalizer) {
			if err := r.cleanupOwnedResources(ctx, route); err != nil {
				outcome = "cleanup_error"
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(route, routeFinalizer)
			if err := r.Update(ctx, route); err != nil {
				outcome = "finalizer_update_error"
				return ctrl.Result{}, err
			}
		}
		r.clearCanaryRuntimeState(route)
		clearRouteMetrics(route)
		outcome = "deleted"
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(route, routeFinalizer) {
		controllerutil.AddFinalizer(route, routeFinalizer)
		if err := r.Update(ctx, route); err != nil {
			outcome = "finalizer_add_error"
			return ctrl.Result{}, err
		}
	}

	prevCertCond := r.findCondition(route, string(platformv1alpha1.RouteConditionCertificateHealthy))
	certStatus, certReason, certMessage, err := r.evaluateGatewayCertificate(ctx, route)
	if err != nil {
		outcome = "cert_check_error"
		return ctrl.Result{}, err
	}
	r.setCondition(route, string(platformv1alpha1.RouteConditionCertificateHealthy), certStatus, certReason, certMessage)
	if r.Recorder != nil && conditionTransitioned(prevCertCond, certStatus, certReason) {
		eventType := corev1.EventTypeNormal
		if certStatus != metav1.ConditionTrue {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Event(route, eventType, certReason, certMessage)
	}

	conflicts, err := r.evaluateRouteConflicts(ctx, route)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(conflicts) > 0 {
		msg := fmt.Sprintf("Conflicting route rules detected: %s", strings.Join(conflicts, "; "))
		route.Status.Phase = platformv1alpha1.RoutePhaseDegraded
		route.Status.ManagedResources = 0
		r.setCondition(route, string(platformv1alpha1.RouteConditionSynced), metav1.ConditionFalse, "RouteConflict", msg)
		r.setCondition(route, string(platformv1alpha1.RouteConditionReady), metav1.ConditionFalse, "RouteConflict", msg)
		if err := r.Status().Update(ctx, route); err != nil {
			log.Error(err, "Failed to update Route status for conflict")
			outcome = "status_update_error"
			return ctrl.Result{}, err
		}
		setRoutePhaseMetric(route, route.Status.Phase)
		setRouteBackendHealthState(route, "conflict")
		setManagedResourcesMetric(route, route.Status.ManagedResources)
		if r.Recorder != nil {
			r.Recorder.Event(route, corev1.EventTypeWarning, "RouteConflict", msg)
		}
		outcome = "conflict"
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	backendHealth, err := r.evaluateBackendHealth(ctx, route)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !backendHealth.allReady() {
		phase, reason, message := backendHealth.phaseReasonAndMessage()
		route.Status.Phase = phase
		route.Status.ManagedResources = 0
		r.setCondition(route, string(platformv1alpha1.RouteConditionSynced), metav1.ConditionFalse, reason, message)
		r.setCondition(route, string(platformv1alpha1.RouteConditionReady), metav1.ConditionFalse, reason, message)
		if err := r.Status().Update(ctx, route); err != nil {
			log.Error(err, "Failed to update Route status for backend health")
			outcome = "status_update_error"
			return ctrl.Result{}, err
		}
		setRoutePhaseMetric(route, route.Status.Phase)
		if phase == platformv1alpha1.RoutePhasePending {
			setRouteBackendHealthState(route, "missing_service")
		} else {
			setRouteBackendHealthState(route, "zero_endpoints")
		}
		setManagedResourcesMetric(route, route.Status.ManagedResources)
		if r.Recorder != nil {
			r.Recorder.Event(route, corev1.EventTypeWarning, reason, message)
		}
		outcome = "backend_blocked"
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	route.Status.Phase = platformv1alpha1.RoutePhaseProvisioning
	effectiveCanary := route.Spec.Canary
	canaryProbeRequeue := time.Duration(0)
	if route.Spec.Canary != nil && route.Spec.Canary.RollbackOn5xx != nil {
		rollbackActive, probeRequeue, err := r.evaluateCanaryRollback(ctx, route)
		if err != nil {
			return ctrl.Result{}, err
		}
		canaryProbeRequeue = probeRequeue
		if rollbackActive {
			effectiveCanary = nil
		}
	}

	// Reconcile child resources
	managedCount := 0
	var reconcileErr error

	// 1. HTTPRoutes (one per rule)
	for i, rule := range route.Spec.Rules {
		if err := r.reconcileHTTPRoute(ctx, route, i, rule, effectiveCanary); err != nil {
			log.Error(err, "Failed to reconcile HTTPRoute", "index", i)
			reconcileErr = err
		} else {
			managedCount++
		}
	}

	// 2. Redirect HTTPRoutes (one per unique host)
	if route.Spec.SSLRedirect == nil || *route.Spec.SSLRedirect {
		uniqueHosts := uniqueHostsFromRules(route.Spec.Rules)
		for i, host := range uniqueHosts {
			if err := r.reconcileRedirectHTTPRoute(ctx, route, i, host); err != nil {
				log.Error(err, "Failed to reconcile redirect HTTPRoute", "host", host)
				reconcileErr = err
			} else {
				managedCount++
			}
		}
	}

	// 3. VirtualService (if timeout, CORS, retries, or canary split)
	if route.Spec.Timeout != "" || route.Spec.CORS != nil || route.Spec.Retries != nil || effectiveCanary != nil {
		if err := r.reconcileVirtualService(ctx, route, effectiveCanary); err != nil {
			log.Error(err, "Failed to reconcile VirtualService")
			reconcileErr = err
		} else {
			managedCount++
		}
	}

	// 4. DestinationRules (for HTTPS backends)
	drIndex := 0
	for _, rule := range route.Spec.Rules {
		for _, backend := range routeRuleBackends(rule, effectiveCanary) {
			protocol := backend.Protocol
			if protocol == "" {
				protocol = "HTTP"
			}
			if protocol == "HTTPS" {
				if err := r.reconcileDestinationRule(ctx, route, drIndex, backend); err != nil {
					log.Error(err, "Failed to reconcile DestinationRule")
					reconcileErr = err
				} else {
					managedCount++
				}
				drIndex++
			}
		}
	}

	// 5. EnvoyFilters (if maxBodySize)
	if route.Spec.MaxBodySize != "" {
		for i, rule := range route.Spec.Rules {
			if err := r.reconcileEnvoyFilter(ctx, route, i, rule); err != nil {
				log.Error(err, "Failed to reconcile EnvoyFilter", "index", i)
				reconcileErr = err
			} else {
				managedCount++
			}
		}
	}

	// 6. Authentication Integration (if auth enabled)
	if route.Spec.Auth != nil {
		if err := r.reconcileRequestAuthentication(ctx, route); err != nil {
			log.Error(err, "Failed to reconcile RequestAuthentication")
			reconcileErr = err
		} else {
			managedCount++
		}
		if err := r.reconcileAuthorizationPolicy(ctx, route); err != nil {
			log.Error(err, "Failed to reconcile AuthorizationPolicy")
			reconcileErr = err
		} else {
			managedCount++
		}
	} else {
		if err := r.deleteAuthResources(ctx, route); err != nil {
			log.Error(err, "Failed to delete auth resources")
			reconcileErr = err
		}
	}

	// 7. Rate limiting (if enabled)
	if route.Spec.RateLimit != nil {
		if err := r.reconcileRateLimitEnvoyFilter(ctx, route); err != nil {
			log.Error(err, "Failed to reconcile rate limit EnvoyFilter")
			reconcileErr = err
		} else {
			managedCount++
		}
	} else {
		if err := r.deleteRateLimitEnvoyFilter(ctx, route); err != nil {
			log.Error(err, "Failed to delete rate limit EnvoyFilter")
			reconcileErr = err
		}
	}

	// Update status
	route.Status.ManagedResources = managedCount
	if reconcileErr != nil {
		route.Status.Phase = platformv1alpha1.RoutePhaseDegraded
		r.setCondition(route, string(platformv1alpha1.RouteConditionSynced), metav1.ConditionFalse, "ReconcileError", reconcileErr.Error())
		r.setCondition(route, string(platformv1alpha1.RouteConditionReady), metav1.ConditionFalse, "NotReady", "Some resources failed to reconcile")
		if r.Recorder != nil {
			r.Recorder.Event(route, corev1.EventTypeWarning, "ReconcileError", reconcileErr.Error())
		}
	} else {
		route.Status.Phase = platformv1alpha1.RoutePhaseActive
		r.setCondition(route, string(platformv1alpha1.RouteConditionSynced), metav1.ConditionTrue, "ReconcileSuccess", "All resources synced")
		r.setCondition(route, string(platformv1alpha1.RouteConditionReady), metav1.ConditionTrue, "Available", "All resources available")
		if r.Recorder != nil {
			r.Recorder.Event(route, corev1.EventTypeNormal, "RouteActive", "All resources synced and backend dependencies are healthy")
		}
	}

	if err := r.Status().Update(ctx, route); err != nil {
		log.Error(err, "Failed to update Route status")
		outcome = "status_update_error"
		return ctrl.Result{}, err
	}

	setRoutePhaseMetric(route, route.Status.Phase)
	setRouteBackendHealthState(route, "healthy")
	setManagedResourcesMetric(route, route.Status.ManagedResources)
	if reconcileErr != nil {
		outcome = "reconcile_error"
	} else {
		outcome = "success"
	}

	if reconcileErr != nil {
		return ctrl.Result{}, reconcileErr
	}
	if canaryProbeRequeue > 0 {
		return ctrl.Result{RequeueAfter: canaryProbeRequeue}, nil
	}
	return ctrl.Result{}, nil
}

func conditionTransitioned(previous *metav1.Condition, newStatus metav1.ConditionStatus, newReason string) bool {
	if previous == nil {
		return true
	}
	return previous.Status != newStatus || previous.Reason != newReason
}

type backendHealthSummary struct {
	MissingServices []string
	ZeroEndpoints   []string
}

func (s backendHealthSummary) allReady() bool {
	return len(s.MissingServices) == 0 && len(s.ZeroEndpoints) == 0
}

func (s backendHealthSummary) phaseReasonAndMessage() (platformv1alpha1.RoutePhase, string, string) {
	if len(s.MissingServices) > 0 {
		return platformv1alpha1.RoutePhasePending, "BackendServiceMissing", fmt.Sprintf("Waiting for backend Services: %s", strings.Join(s.MissingServices, ", "))
	}
	if len(s.ZeroEndpoints) > 0 {
		return platformv1alpha1.RoutePhaseDegraded, "BackendNoEndpoints", fmt.Sprintf("Backends have zero ready endpoints: %s", strings.Join(s.ZeroEndpoints, ", "))
	}
	return platformv1alpha1.RoutePhaseProvisioning, "BackendsReady", "All backend dependencies are ready"
}

func (r *RouteReconciler) evaluateBackendHealth(ctx context.Context, route *platformv1alpha1.Route) (backendHealthSummary, error) {
	seenServices := make(map[string]struct{})
	summary := backendHealthSummary{}

	for _, rule := range route.Spec.Rules {
		for _, backend := range routeRuleBackends(rule, route.Spec.Canary) {
			serviceName := backend.ServiceName
			serviceKey := route.Namespace + "/" + serviceName
			if _, seen := seenServices[serviceKey]; seen {
				continue
			}
			seenServices[serviceKey] = struct{}{}

			svc := &corev1.Service{}
			err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: route.Namespace}, svc)
			if errors.IsNotFound(err) {
				summary.MissingServices = append(summary.MissingServices, serviceName)
				continue
			}
			if err != nil {
				return summary, err
			}

			ep := &corev1.Endpoints{}
			err = r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: route.Namespace}, ep)
			if errors.IsNotFound(err) {
				summary.ZeroEndpoints = append(summary.ZeroEndpoints, serviceName)
				continue
			}
			if err != nil {
				return summary, err
			}

			if !hasReadyEndpoints(ep) {
				summary.ZeroEndpoints = append(summary.ZeroEndpoints, serviceName)
			}
		}
	}

	return summary, nil
}

func hasReadyEndpoints(ep *corev1.Endpoints) bool {
	for _, subset := range ep.Subsets {
		if len(subset.Addresses) > 0 {
			return true
		}
	}
	return false
}

func (r *RouteReconciler) evaluateGatewayCertificate(ctx context.Context, route *platformv1alpha1.Route) (metav1.ConditionStatus, string, string, error) {
	gwName, gwNs := r.gatewayRef(route)
	gw := &gatewayv1.Gateway{}
	err := r.Get(ctx, types.NamespacedName{Name: gwName, Namespace: gwNs}, gw)
	if errors.IsNotFound(err) {
		return metav1.ConditionFalse, "GatewayNotFound", fmt.Sprintf("Gateway %s/%s not found for certificate check", gwNs, gwName), nil
	}
	if err != nil {
		return metav1.ConditionUnknown, "CertificateCheckFailed", fmt.Sprintf("failed to get Gateway %s/%s: %v", gwNs, gwName, err), err
	}

	var earliestExpiry *time.Time
	hasTLSRef := false
	for _, listener := range gw.Spec.Listeners {
		if string(listener.Protocol) != "HTTPS" || listener.TLS == nil {
			continue
		}

		for _, ref := range listener.TLS.CertificateRefs {
			if ref.Group != nil && string(*ref.Group) != "" {
				continue
			}
			if ref.Kind != nil && string(*ref.Kind) != "Secret" {
				continue
			}
			hasTLSRef = true

			secretNs := gwNs
			if ref.Namespace != nil && string(*ref.Namespace) != "" {
				secretNs = string(*ref.Namespace)
			}

			secret := &corev1.Secret{}
			err := r.Get(ctx, types.NamespacedName{Name: string(ref.Name), Namespace: secretNs}, secret)
			if errors.IsNotFound(err) {
				return metav1.ConditionFalse, "CertificateSecretMissing", fmt.Sprintf("TLS secret %s/%s not found", secretNs, string(ref.Name)), nil
			}
			if err != nil {
				return metav1.ConditionUnknown, "CertificateCheckFailed", fmt.Sprintf("failed to get TLS secret %s/%s: %v", secretNs, string(ref.Name), err), err
			}

			notAfter, err := parseTLSCertNotAfter(secret)
			if err != nil {
				return metav1.ConditionFalse, "CertificateParseError", err.Error(), nil
			}

			if earliestExpiry == nil || notAfter.Before(*earliestExpiry) {
				t := notAfter
				earliestExpiry = &t
			}
		}
	}

	if !hasTLSRef {
		return metav1.ConditionTrue, "CertificateNotConfigured", "Gateway has no HTTPS TLS certificateRefs to monitor", nil
	}
	if earliestExpiry == nil {
		return metav1.ConditionTrue, "CertificateNotConfigured", "Gateway has no supported Secret certificateRefs to monitor", nil
	}

	status, reason, message := certificateExpiryState(time.Now(), *earliestExpiry, certWarnBefore)
	return metav1.ConditionStatus(status), reason, message, nil
}

type routeRuleIdentity struct {
	GatewayNamespace string
	GatewayName      string
	Host             string
	Path             string
}

func normalizeRoutePath(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func ruleIdentity(route *platformv1alpha1.Route, rule platformv1alpha1.RouteRule) routeRuleIdentity {
	gwName := "istio-gateway"
	gwNs := "istio-system"
	if route.Spec.Gateway != nil {
		if route.Spec.Gateway.Name != "" {
			gwName = route.Spec.Gateway.Name
		}
		if route.Spec.Gateway.Namespace != "" {
			gwNs = route.Spec.Gateway.Namespace
		}
	}

	return routeRuleIdentity{
		GatewayNamespace: gwNs,
		GatewayName:      gwName,
		Host:             rule.Host,
		Path:             normalizeRoutePath(rule.Path),
	}
}

func routePrecedes(a, b *platformv1alpha1.Route) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	return a.Name < b.Name
}

func (r *RouteReconciler) evaluateRouteConflicts(ctx context.Context, route *platformv1alpha1.Route) ([]string, error) {
	allRoutes := &platformv1alpha1.RouteList{}
	if err := r.List(ctx, allRoutes); err != nil {
		return nil, err
	}

	currentRules := make(map[routeRuleIdentity]struct{})
	for _, rule := range route.Spec.Rules {
		currentRules[ruleIdentity(route, rule)] = struct{}{}
	}

	conflicts := make(map[string]struct{})
	for i := range allRoutes.Items {
		other := &allRoutes.Items[i]
		if other.Name == route.Name && other.Namespace == route.Namespace {
			continue
		}
		if !other.DeletionTimestamp.IsZero() {
			continue
		}
		if !routePrecedes(route, other) {
			for _, otherRule := range other.Spec.Rules {
				id := ruleIdentity(other, otherRule)
				if _, exists := currentRules[id]; exists {
					conflicts[fmt.Sprintf("%s/%s host=%s path=%s gateway=%s/%s", other.Namespace, other.Name, id.Host, id.Path, id.GatewayNamespace, id.GatewayName)] = struct{}{}
				}
			}
		}
	}

	var out []string
	for c := range conflicts {
		out = append(out, c)
	}
	sort.Strings(out)
	return out, nil
}

func (r *RouteReconciler) gatewayRef(route *platformv1alpha1.Route) (string, string) {
	name := "istio-gateway"
	ns := "istio-system"
	if route.Spec.Gateway != nil {
		if route.Spec.Gateway.Name != "" {
			name = route.Spec.Gateway.Name
		}
		if route.Spec.Gateway.Namespace != "" {
			ns = route.Spec.Gateway.Namespace
		}
	}
	return name, ns
}

func (r *RouteReconciler) reconcileHTTPRoute(ctx context.Context, route *platformv1alpha1.Route, index int, rule platformv1alpha1.RouteRule, canary *platformv1alpha1.RouteCanary) error {
	gwName, gwNs := r.gatewayRef(route)
	ns := gatewayv1.Namespace(gwNs)
	sectionName := gatewayv1.SectionName("https")
	path := rule.Path
	if path == "" {
		path = "/"
	}
	pathType := gatewayv1.PathMatchPathPrefix
	port := gatewayv1.PortNumber(rule.Backend.ServicePort)
	backendRefs := []gatewayv1.HTTPBackendRef{{
		BackendRef: gatewayv1.BackendRef{
			BackendObjectReference: gatewayv1.BackendObjectReference{
				Name: gatewayv1.ObjectName(rule.Backend.ServiceName),
				Port: &port,
			},
		},
	}}

	if canary != nil {
		canaryWeight := canary.Weight
		primaryWeight := int32(100 - canaryWeight)
		backendRefs[0].Weight = &primaryWeight

		canaryPort := gatewayv1.PortNumber(canary.Backend.ServicePort)
		backendRefs = append(backendRefs, gatewayv1.HTTPBackendRef{
			BackendRef: gatewayv1.BackendRef{
				BackendObjectReference: gatewayv1.BackendObjectReference{
					Name: gatewayv1.ObjectName(canary.Backend.ServiceName),
					Port: &canaryPort,
				},
				Weight: &canaryWeight,
			},
		})
	}

	hr := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", route.Name, index),
			Namespace: route.Namespace,
			Labels:    r.managedLabels(route),
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{{
					Name:        gatewayv1.ObjectName(gwName),
					Namespace:   &ns,
					SectionName: &sectionName,
				}},
			},
			Hostnames: []gatewayv1.Hostname{gatewayv1.Hostname(rule.Host)},
			Rules: []gatewayv1.HTTPRouteRule{{
				Matches: []gatewayv1.HTTPRouteMatch{{
					Path: &gatewayv1.HTTPPathMatch{
						Type:  &pathType,
						Value: &path,
					},
				}},
				Filters: buildGatewayHTTPHeaderFilters(rule.Headers),
				BackendRefs: backendRefs,
			}},
		},
	}

	return r.createOrUpdate(ctx, route, hr)
}

func (r *RouteReconciler) reconcileRedirectHTTPRoute(ctx context.Context, route *platformv1alpha1.Route, index int, host string) error {
	gwName, gwNs := r.gatewayRef(route)
	ns := gatewayv1.Namespace(gwNs)
	sectionName := gatewayv1.SectionName("http")
	scheme := "https"
	statusCode := 301

	hr := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-redirect-%d", route.Name, index),
			Namespace: route.Namespace,
			Labels:    r.managedLabels(route),
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{{
					Name:        gatewayv1.ObjectName(gwName),
					Namespace:   &ns,
					SectionName: &sectionName,
				}},
			},
			Hostnames: []gatewayv1.Hostname{gatewayv1.Hostname(host)},
			Rules: []gatewayv1.HTTPRouteRule{{
				Filters: []gatewayv1.HTTPRouteFilter{{
					Type: gatewayv1.HTTPRouteFilterRequestRedirect,
					RequestRedirect: &gatewayv1.HTTPRequestRedirectFilter{
						Scheme:     &scheme,
						StatusCode: &statusCode,
					},
				}},
			}},
		},
	}

	return r.createOrUpdate(ctx, route, hr)
}

func (r *RouteReconciler) reconcileVirtualService(ctx context.Context, route *platformv1alpha1.Route, canary *platformv1alpha1.RouteCanary) error {
	gwName, gwNs := r.gatewayRef(route)

	// Build HTTP routes
	var httpRoutes []*istionetv1.HTTPRoute
	for _, rule := range route.Spec.Rules {
		path := rule.Path
		if path == "" {
			path = "/"
		}
		httpRoute := &istionetv1.HTTPRoute{
			Match: []*istionetv1.HTTPMatchRequest{{
				Uri:     &istionetv1.StringMatch{MatchType: &istionetv1.StringMatch_Prefix{Prefix: path}},
				Headers: map[string]*istionetv1.StringMatch{":authority": {MatchType: &istionetv1.StringMatch_Exact{Exact: rule.Host}}},
			}},
			Route: buildIstioRouteDestinations(route.Namespace, rule.Backend, canary),
		}

		if route.Spec.Timeout != "" {
			d, err := parseDuration(route.Spec.Timeout)
			if err == nil {
				httpRoute.Timeout = d
			}
		}

		if route.Spec.CORS != nil {
			corsPolicy := &istionetv1.CorsPolicy{}
			if len(route.Spec.CORS.AllowOrigins) > 0 {
				for _, o := range route.Spec.CORS.AllowOrigins {
					corsPolicy.AllowOrigins = append(corsPolicy.AllowOrigins, &istionetv1.StringMatch{
						MatchType: &istionetv1.StringMatch_Regex{Regex: o},
					})
				}
			}
			if len(route.Spec.CORS.AllowMethods) > 0 {
				corsPolicy.AllowMethods = route.Spec.CORS.AllowMethods
			}
			if len(route.Spec.CORS.AllowHeaders) > 0 {
				corsPolicy.AllowHeaders = route.Spec.CORS.AllowHeaders
			}
			if route.Spec.CORS.MaxAge != "" {
				d, err := parseDuration(route.Spec.CORS.MaxAge)
				if err == nil {
					corsPolicy.MaxAge = d
				}
			}
			httpRoute.CorsPolicy = corsPolicy
		}
		httpRoute.Headers = buildIstioHeaders(rule.Headers)

		httpRoute.Retries = buildIstioHTTPRetry(route.Spec.Retries)

		httpRoutes = append(httpRoutes, httpRoute)
	}

	// Deduplicate hosts
	hosts := uniqueHostsFromRules(route.Spec.Rules)

	vs := &networkingv1.VirtualService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      route.Name,
			Namespace: route.Namespace,
			Labels:    r.managedLabels(route),
		},
	}
	vs.Spec.Hosts = hosts
	vs.Spec.Gateways = []string{fmt.Sprintf("%s/%s", gwNs, gwName)}
	vs.Spec.Http = httpRoutes

	return r.createOrUpdate(ctx, route, vs)
}

func buildIstioHTTPRetry(retries *platformv1alpha1.RouteRetries) *istionetv1.HTTPRetry {
	if retries == nil {
		return nil
	}

	httpRetry := &istionetv1.HTTPRetry{Attempts: retries.Attempts}
	if retries.RetryOn != "" {
		httpRetry.RetryOn = retries.RetryOn
	}
	if retries.PerTryTimeout != "" {
		d, err := parseDuration(retries.PerTryTimeout)
		if err == nil {
			httpRetry.PerTryTimeout = d
		}
	}

	return httpRetry
}

func routeRuleBackends(rule platformv1alpha1.RouteRule, canary *platformv1alpha1.RouteCanary) []platformv1alpha1.RouteBackend {
	backends := []platformv1alpha1.RouteBackend{rule.Backend}
	if canary != nil {
		backends = append(backends, canary.Backend)
	}
	return backends
}

func buildIstioRouteDestinations(routeNamespace string, primary platformv1alpha1.RouteBackend, canary *platformv1alpha1.RouteCanary) []*istionetv1.HTTPRouteDestination {
	dests := []*istionetv1.HTTPRouteDestination{{
		Destination: &istionetv1.Destination{
			Host: fmt.Sprintf("%s.%s.svc.cluster.local", primary.ServiceName, routeNamespace),
			Port: &istionetv1.PortSelector{Number: uint32(primary.ServicePort)},
		},
	}}

	if canary == nil {
		return dests
	}

	primaryWeight := int32(100 - canary.Weight)
	dests[0].Weight = primaryWeight
	dests = append(dests, &istionetv1.HTTPRouteDestination{
		Destination: &istionetv1.Destination{
			Host: fmt.Sprintf("%s.%s.svc.cluster.local", canary.Backend.ServiceName, routeNamespace),
			Port: &istionetv1.PortSelector{Number: uint32(canary.Backend.ServicePort)},
		},
		Weight: canary.Weight,
	})

	return dests
}

func (r *RouteReconciler) evaluateCanaryRollback(ctx context.Context, route *platformv1alpha1.Route) (bool, time.Duration, error) {
	if route.Spec.Canary == nil || route.Spec.Canary.RollbackOn5xx == nil {
		return false, 0, nil
	}

	policy := route.Spec.Canary.RollbackOn5xx
	probeInterval := defaultCanaryProbeInterval
	if policy.IntervalSeconds != nil {
		probeInterval = time.Duration(*policy.IntervalSeconds) * time.Second
	}

	cooldown := defaultCanaryCooldown
	if policy.CooldownSeconds != nil {
		cooldown = time.Duration(*policy.CooldownSeconds) * time.Second
	}

	key := routeKey(route)
	now := time.Now()
	r.canaryStateMu.Lock()
	if r.canaryFailureCounts == nil {
		r.canaryFailureCounts = make(map[string]int)
	}
	if r.canaryRollbackUntil == nil {
		r.canaryRollbackUntil = make(map[string]time.Time)
	}
	if until, ok := r.canaryRollbackUntil[key]; ok && until.After(now) {
		r.canaryStateMu.Unlock()
		return true, probeInterval, nil
	}
	r.canaryStateMu.Unlock()

	statusCode, err := r.probeCanaryBackend(ctx, route, route.Spec.Canary.Backend, policy.ProbePath)
	isFiveXX := err != nil || statusCode >= http.StatusInternalServerError

	triggeredRollback := false
	recovered := false
	r.canaryStateMu.Lock()
	count := r.canaryFailureCounts[key]
	if isFiveXX {
		count++
	} else {
		count = 0
	}
	r.canaryFailureCounts[key] = count

	if count >= int(policy.FiveXXThreshold) {
		r.canaryRollbackUntil[key] = now.Add(cooldown)
		r.canaryFailureCounts[key] = 0
		triggeredRollback = true
	}

	rollbackActive := false
	if until, ok := r.canaryRollbackUntil[key]; ok {
		if until.After(now) {
			rollbackActive = true
		} else {
			delete(r.canaryRollbackUntil, key)
			recovered = true
		}
	}
	r.canaryStateMu.Unlock()

	if triggeredRollback && r.Recorder != nil {
		msg := fmt.Sprintf("Canary rollback activated: %d consecutive 5xx probe failures", policy.FiveXXThreshold)
		r.Recorder.Event(route, corev1.EventTypeWarning, "CanaryRollback", msg)
	}
	if recovered && r.Recorder != nil {
		r.Recorder.Event(route, corev1.EventTypeNormal, "CanaryRecovered", "Canary rollback cooldown elapsed; re-enabling canary traffic")
	}

	return rollbackActive, probeInterval, nil
}

func (r *RouteReconciler) probeCanaryBackend(ctx context.Context, route *platformv1alpha1.Route, backend platformv1alpha1.RouteBackend, probePath string) (int, error) {
	url := canaryBackendProbeURL(route.Namespace, backend, probePath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

func normalizeCanaryProbePath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

func canaryBackendProbeURL(namespace string, backend platformv1alpha1.RouteBackend, probePath string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d%s", backend.ServiceName, namespace, backend.ServicePort, normalizeCanaryProbePath(probePath))
}

func routeKey(route *platformv1alpha1.Route) string {
	return route.Namespace + "/" + route.Name
}

func (r *RouteReconciler) clearCanaryRuntimeState(route *platformv1alpha1.Route) {
	key := routeKey(route)
	r.canaryStateMu.Lock()
	defer r.canaryStateMu.Unlock()
	if r.canaryFailureCounts != nil {
		delete(r.canaryFailureCounts, key)
	}
	if r.canaryRollbackUntil != nil {
		delete(r.canaryRollbackUntil, key)
	}
}

func buildGatewayHTTPHeaderFilters(headers *platformv1alpha1.RouteHeaders) []gatewayv1.HTTPRouteFilter {
	if headers == nil {
		return nil
	}

	filters := make([]gatewayv1.HTTPRouteFilter, 0, 2)
	if requestFilter := buildGatewayHTTPHeaderFilter(headers.Request); requestFilter != nil {
		filters = append(filters, gatewayv1.HTTPRouteFilter{
			Type:                  gatewayv1.HTTPRouteFilterRequestHeaderModifier,
			RequestHeaderModifier: requestFilter,
		})
	}
	if responseFilter := buildGatewayHTTPHeaderFilter(headers.Response); responseFilter != nil {
		filters = append(filters, gatewayv1.HTTPRouteFilter{
			Type:                   gatewayv1.HTTPRouteFilterResponseHeaderModifier,
			ResponseHeaderModifier: responseFilter,
		})
	}

	if len(filters) == 0 {
		return nil
	}
	return filters
}

func buildGatewayHTTPHeaderFilter(ops *platformv1alpha1.RouteHeaderOperations) *gatewayv1.HTTPHeaderFilter {
	if !hasHeaderOperations(ops) {
		return nil
	}

	out := &gatewayv1.HTTPHeaderFilter{}
	for _, k := range sortedMapKeys(ops.Set) {
		out.Set = append(out.Set, gatewayv1.HTTPHeader{Name: gatewayv1.HTTPHeaderName(k), Value: ops.Set[k]})
	}
	for _, k := range sortedMapKeys(ops.Add) {
		out.Add = append(out.Add, gatewayv1.HTTPHeader{Name: gatewayv1.HTTPHeaderName(k), Value: ops.Add[k]})
	}
	if len(ops.Remove) > 0 {
		out.Remove = append([]string(nil), ops.Remove...)
	}

	return out
}

func buildIstioHeaders(headers *platformv1alpha1.RouteHeaders) *istionetv1.Headers {
	if headers == nil {
		return nil
	}

	requestOps := buildIstioHeaderOperations(headers.Request)
	responseOps := buildIstioHeaderOperations(headers.Response)
	if requestOps == nil && responseOps == nil {
		return nil
	}

	return &istionetv1.Headers{
		Request:  requestOps,
		Response: responseOps,
	}
}

func buildIstioHeaderOperations(ops *platformv1alpha1.RouteHeaderOperations) *istionetv1.Headers_HeaderOperations {
	if !hasHeaderOperations(ops) {
		return nil
	}

	out := &istionetv1.Headers_HeaderOperations{}
	if len(ops.Set) > 0 {
		out.Set = make(map[string]string, len(ops.Set))
		for k, v := range ops.Set {
			out.Set[k] = v
		}
	}
	if len(ops.Add) > 0 {
		out.Add = make(map[string]string, len(ops.Add))
		for k, v := range ops.Add {
			out.Add[k] = v
		}
	}
	if len(ops.Remove) > 0 {
		out.Remove = append([]string(nil), ops.Remove...)
	}

	return out
}

func hasHeaderOperations(ops *platformv1alpha1.RouteHeaderOperations) bool {
	return ops != nil && (len(ops.Set) > 0 || len(ops.Add) > 0 || len(ops.Remove) > 0)
}

func sortedMapKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (r *RouteReconciler) reconcileRequestAuthentication(ctx context.Context, route *platformv1alpha1.Route) error {
	if route.Spec.Auth == nil {
		return nil
	}

	gwName, gwNs := r.gatewayRef(route)
	forwardToken := true
	if route.Spec.Auth.ForwardOriginalToken != nil {
		forwardToken = *route.Spec.Auth.ForwardOriginalToken
	}

	ra := &securityv1beta1.RequestAuthentication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-jwt", route.Name),
			Namespace: gwNs,
			Labels:    r.managedLabels(route),
		},
	}
	ra.Spec.Selector = &istiotypev1beta1.WorkloadSelector{MatchLabels: map[string]string{"gateway.networking.k8s.io/gateway-name": gwName}}
	ra.Spec.JwtRules = []*istiosecv1beta1.JWTRule{{
		Issuer:               route.Spec.Auth.Issuer,
		JwksUri:              route.Spec.Auth.JWKSURI,
		Audiences:            append([]string(nil), route.Spec.Auth.Audiences...),
		ForwardOriginalToken: forwardToken,
	}}

	return r.createOrUpdateCrossNS(ctx, route, ra)
}

func (r *RouteReconciler) reconcileAuthorizationPolicy(ctx context.Context, route *platformv1alpha1.Route) error {
	if route.Spec.Auth == nil {
		return nil
	}

	gwName, gwNs := r.gatewayRef(route)
	ap := &securityv1beta1.AuthorizationPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-authz", route.Name),
			Namespace: gwNs,
			Labels:    r.managedLabels(route),
		},
	}
	ap.Spec.Selector = &istiotypev1beta1.WorkloadSelector{MatchLabels: map[string]string{"gateway.networking.k8s.io/gateway-name": gwName}}
	ap.Spec.Action = istiosecv1beta1.AuthorizationPolicy_ALLOW
	ap.Spec.Rules = buildAuthorizationPolicyRules(route.Spec.Rules)

	return r.createOrUpdateCrossNS(ctx, route, ap)
}

func buildAuthorizationPolicyRules(rules []platformv1alpha1.RouteRule) []*istiosecv1beta1.Rule {
	out := make([]*istiosecv1beta1.Rule, 0, len(rules))
	for _, rule := range rules {
		path := authzPathPattern(rule.Path)
		out = append(out, &istiosecv1beta1.Rule{
			From: []*istiosecv1beta1.Rule_From{{
				Source: &istiosecv1beta1.Source{RequestPrincipals: []string{"*"}},
			}},
			To: []*istiosecv1beta1.Rule_To{{
				Operation: &istiosecv1beta1.Operation{
					Hosts: []string{rule.Host},
					Paths: []string{path},
				},
			}},
		})
	}
	return out
}

func authzPathPattern(path string) string {
	if path == "" || path == "/" {
		return "/*"
	}
	if strings.HasSuffix(path, "*") {
		return path
	}
	if strings.HasSuffix(path, "/") {
		return path + "*"
	}
	return path + "*"
}

func (r *RouteReconciler) deleteAuthResources(ctx context.Context, route *platformv1alpha1.Route) error {
	_, gwNs := r.gatewayRef(route)
	for _, obj := range []client.Object{
		&securityv1beta1.RequestAuthentication{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-jwt", route.Name), Namespace: gwNs}},
		&securityv1beta1.AuthorizationPolicy{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-authz", route.Name), Namespace: gwNs}},
	} {
		if err := r.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *RouteReconciler) reconcileRateLimitEnvoyFilter(ctx context.Context, route *platformv1alpha1.Route) error {
	if route.Spec.RateLimit == nil {
		return nil
	}

	gwName, gwNs := r.gatewayRef(route)
	fillInterval, err := rateLimitFillInterval(route.Spec.RateLimit.Unit)
	if err != nil {
		return err
	}

	tokensPerFill := int64(route.Spec.RateLimit.RequestsPerUnit)
	maxTokens := tokensPerFill
	if route.Spec.RateLimit.Burst != nil {
		maxTokens = int64(*route.Spec.RateLimit.Burst)
	}

	patches := []*istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectPatch{buildRateLimitHTTPFilterPatch(route.Name)}
	for _, host := range uniqueHostsFromRules(route.Spec.Rules) {
		patches = append(patches, buildRateLimitRoutePatch(route.Name, host, maxTokens, tokensPerFill, fillInterval))
	}

	ef := &istiov1alpha3.EnvoyFilter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-rate-limit", route.Name),
			Namespace: gwNs,
			Labels:    r.managedLabels(route),
		},
	}
	ef.Spec.WorkloadSelector = &istionetv1alpha3.WorkloadSelector{
		Labels: map[string]string{"gateway.networking.k8s.io/gateway-name": gwName},
	}
	ef.Spec.ConfigPatches = patches

	return r.createOrUpdateCrossNS(ctx, route, ef)
}

func buildRateLimitHTTPFilterPatch(routeName string) *istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectPatch {
	return &istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectPatch{
		ApplyTo: istionetv1alpha3.EnvoyFilter_HTTP_FILTER,
		Match: &istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectMatch{
			Context: istionetv1alpha3.EnvoyFilter_GATEWAY,
			ObjectTypes: &istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectMatch_Listener{
				Listener: &istionetv1alpha3.EnvoyFilter_ListenerMatch{
					FilterChain: &istionetv1alpha3.EnvoyFilter_ListenerMatch_FilterChainMatch{
						Filter: &istionetv1alpha3.EnvoyFilter_ListenerMatch_FilterMatch{
							Name: "envoy.filters.network.http_connection_manager",
							SubFilter: &istionetv1alpha3.EnvoyFilter_ListenerMatch_SubFilterMatch{
								Name: "envoy.filters.http.router",
							},
						},
					},
				},
			},
		},
		Patch: &istionetv1alpha3.EnvoyFilter_Patch{
			Operation: istionetv1alpha3.EnvoyFilter_Patch_INSERT_BEFORE,
			Value: mustStruct(map[string]any{
				"name": "envoy.filters.http.local_ratelimit",
				"typed_config": map[string]any{
					"@type":       "type.googleapis.com/envoy.extensions.filters.http.local_ratelimit.v3.LocalRateLimit",
					"stat_prefix": fmt.Sprintf("%s_local_rate_limiter", routeName),
				},
			}),
		},
	}
}

func buildRateLimitRoutePatch(routeName, host string, maxTokens, tokensPerFill int64, fillInterval string) *istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectPatch {
	statPrefix := strings.NewReplacer(".", "_", "-", "_").Replace(host)
	return &istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectPatch{
		ApplyTo: istionetv1alpha3.EnvoyFilter_HTTP_ROUTE,
		Match: &istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectMatch{
			Context: istionetv1alpha3.EnvoyFilter_GATEWAY,
			ObjectTypes: &istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectMatch_RouteConfiguration{
				RouteConfiguration: &istionetv1alpha3.EnvoyFilter_RouteConfigurationMatch{
					Vhost: &istionetv1alpha3.EnvoyFilter_RouteConfigurationMatch_VirtualHostMatch{
						Name: fmt.Sprintf("%s:443", host),
					},
				},
			},
		},
		Patch: &istionetv1alpha3.EnvoyFilter_Patch{
			Operation: istionetv1alpha3.EnvoyFilter_Patch_MERGE,
			Value: mustStruct(map[string]any{
				"typed_per_filter_config": map[string]any{
					"envoy.filters.http.local_ratelimit": map[string]any{
						"@type":       "type.googleapis.com/envoy.extensions.filters.http.local_ratelimit.v3.LocalRateLimit",
						"stat_prefix": fmt.Sprintf("%s_%s", routeName, statPrefix),
						"token_bucket": map[string]any{
							"max_tokens":      maxTokens,
							"tokens_per_fill": tokensPerFill,
							"fill_interval":   fillInterval,
						},
					},
				},
			}),
		},
	}
}

func rateLimitFillInterval(unit string) (string, error) {
	switch unit {
	case "Second":
		return "1s", nil
	case "Minute":
		return "60s", nil
	case "Hour":
		return "3600s", nil
	default:
		return "", fmt.Errorf("unsupported rateLimit.unit %q", unit)
	}
}

func (r *RouteReconciler) deleteRateLimitEnvoyFilter(ctx context.Context, route *platformv1alpha1.Route) error {
	_, gwNs := r.gatewayRef(route)
	ef := &istiov1alpha3.EnvoyFilter{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-rate-limit", route.Name), Namespace: gwNs}}
	if err := r.Delete(ctx, ef); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *RouteReconciler) reconcileDestinationRule(ctx context.Context, route *platformv1alpha1.Route, index int, backend platformv1alpha1.RouteBackend) error {
	_, gwNs := r.gatewayRef(route)

	dr := &networkingv1.DestinationRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-tls-%d", route.Name, index),
			Namespace: gwNs,
			Labels:    r.managedLabels(route),
		},
	}
	dr.Spec.Host = fmt.Sprintf("%s.%s.svc.cluster.local", backend.ServiceName, route.Namespace)
	dr.Spec.TrafficPolicy = &istionetv1.TrafficPolicy{
		PortLevelSettings: []*istionetv1.TrafficPolicy_PortTrafficPolicy{{
			Port: &istionetv1.PortSelector{Number: uint32(backend.ServicePort)},
			Tls: &istionetv1.ClientTLSSettings{
				Mode:               istionetv1.ClientTLSSettings_SIMPLE,
				InsecureSkipVerify: &wrapperspb_BoolValue{Value: true},
			},
		}},
	}

	// DestinationRule is cross-namespace (in gateway ns), can't use ownerRef
	return r.createOrUpdateCrossNS(ctx, route, dr)
}

func (r *RouteReconciler) reconcileEnvoyFilter(ctx context.Context, route *platformv1alpha1.Route, index int, rule platformv1alpha1.RouteRule) error {
	_, gwNs := r.gatewayRef(route)
	gwName, _ := r.gatewayRef(route)

	maxBytes, err := strconv.ParseUint(route.Spec.MaxBodySize, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid maxBodySize %q: %w", route.Spec.MaxBodySize, err)
	}

	ef := &istiov1alpha3.EnvoyFilter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-body-size-%d", route.Name, index),
			Namespace: gwNs,
			Labels:    r.managedLabels(route),
		},
	}
	ef.Spec.WorkloadSelector = &istionetv1alpha3.WorkloadSelector{
		Labels: map[string]string{"gateway.networking.k8s.io/gateway-name": gwName},
	}
	ef.Spec.ConfigPatches = []*istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectPatch{{
		ApplyTo: istionetv1alpha3.EnvoyFilter_HTTP_ROUTE,
		Match: &istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectMatch{
			Context: istionetv1alpha3.EnvoyFilter_GATEWAY,
			ObjectTypes: &istionetv1alpha3.EnvoyFilter_EnvoyConfigObjectMatch_RouteConfiguration{
				RouteConfiguration: &istionetv1alpha3.EnvoyFilter_RouteConfigurationMatch{
					Vhost: &istionetv1alpha3.EnvoyFilter_RouteConfigurationMatch_VirtualHostMatch{
						Name: fmt.Sprintf("%s:443", rule.Host),
					},
				},
			},
		},
		Patch: &istionetv1alpha3.EnvoyFilter_Patch{
			Operation: istionetv1alpha3.EnvoyFilter_Patch_MERGE,
			Value: mustStruct(map[string]any{
				"per_filter_config": map[string]any{
					"envoy.filters.http.buffer": map[string]any{
						"@type":  "type.googleapis.com/envoy.extensions.filters.http.buffer.v3.BufferPerRoute",
						"buffer": map[string]any{"max_request_bytes": maxBytes},
					},
				},
			}),
		},
	}}

	// EnvoyFilter is cross-namespace (in gateway ns), can't use ownerRef
	return r.createOrUpdateCrossNS(ctx, route, ef)
}

// createOrUpdate creates or updates a resource with ownerReference set.
func (r *RouteReconciler) createOrUpdate(ctx context.Context, route *platformv1alpha1.Route, obj client.Object) error {
	// Set owner reference for garbage collection
	if err := controllerutil.SetControllerReference(route, obj, r.Scheme); err != nil {
		return err
	}

	existing := obj.DeepCopyObject().(client.Object)
	err := r.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, obj)
	}
	if err != nil {
		return err
	}

	obj.SetResourceVersion(existing.GetResourceVersion())
	return r.Update(ctx, obj)
}

// createOrUpdateCrossNS handles resources in other namespaces (no ownerRef, uses labels).
func (r *RouteReconciler) createOrUpdateCrossNS(ctx context.Context, route *platformv1alpha1.Route, obj client.Object) error {
	existing := obj.DeepCopyObject().(client.Object)
	err := r.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, obj)
	}
	if err != nil {
		return err
	}

	obj.SetResourceVersion(existing.GetResourceVersion())
	return r.Update(ctx, obj)
}

// cleanupOwnedResources deletes cross-namespace resources on Route deletion.
func (r *RouteReconciler) cleanupOwnedResources(ctx context.Context, route *platformv1alpha1.Route) error {
	labels := r.managedLabels(route)
	listOpts := []client.ListOption{client.MatchingLabels(labels)}

	// Clean up DestinationRules in gateway namespace
	_, gwNs := r.gatewayRef(route)
	drList := &networkingv1.DestinationRuleList{}
	if err := r.List(ctx, drList, append(listOpts, client.InNamespace(gwNs))...); err == nil {
		for i := range drList.Items {
			_ = r.Delete(ctx, drList.Items[i])
		}
	}

	// Clean up EnvoyFilters in gateway namespace
	efList := &istiov1alpha3.EnvoyFilterList{}
	if err := r.List(ctx, efList, append(listOpts, client.InNamespace(gwNs))...); err == nil {
		for i := range efList.Items {
			_ = r.Delete(ctx, efList.Items[i])
		}
	}

	// Clean up RequestAuthentications in gateway namespace
	raList := &securityv1beta1.RequestAuthenticationList{}
	if err := r.List(ctx, raList, append(listOpts, client.InNamespace(gwNs))...); err == nil {
		for i := range raList.Items {
			_ = r.Delete(ctx, raList.Items[i])
		}
	}

	// Clean up AuthorizationPolicies in gateway namespace
	apList := &securityv1beta1.AuthorizationPolicyList{}
	if err := r.List(ctx, apList, append(listOpts, client.InNamespace(gwNs))...); err == nil {
		for i := range apList.Items {
			_ = r.Delete(ctx, apList.Items[i])
		}
	}

	return nil
}

func (r *RouteReconciler) managedLabels(route *platformv1alpha1.Route) map[string]string {
	return map[string]string{
		managedByLabel: "route-operator",
		routeNameLabel: route.Name,
	}
}

func (r *RouteReconciler) findCondition(route *platformv1alpha1.Route, condType string) *metav1.Condition {
	for i := range route.Status.Conditions {
		if route.Status.Conditions[i].Type == condType {
			c := route.Status.Conditions[i]
			return &c
		}
	}
	return nil
}

func (r *RouteReconciler) setCondition(route *platformv1alpha1.Route, condType string, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	for i, c := range route.Status.Conditions {
		if c.Type == condType {
			if c.Status != status || c.Reason != reason {
				route.Status.Conditions[i].Status = status
				route.Status.Conditions[i].Reason = reason
				route.Status.Conditions[i].Message = message
				route.Status.Conditions[i].LastTransitionTime = now
				route.Status.Conditions[i].ObservedGeneration = route.Generation
			}
			return
		}
	}
	route.Status.Conditions = append(route.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: route.Generation,
	})
}

func uniqueHostsFromRules(rules []platformv1alpha1.RouteRule) []string {
	seen := make(map[string]bool)
	var hosts []string
	for _, r := range rules {
		if !seen[r.Host] {
			seen[r.Host] = true
			hosts = append(hosts, r.Host)
		}
	}
	return hosts
}

// SetupWithManager sets up the controller with the Manager.
func (r *RouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.Route{}).
		Owns(&gatewayv1.HTTPRoute{}).
		Owns(&securityv1beta1.RequestAuthentication{}).
		Owns(&securityv1beta1.AuthorizationPolicy{}).
		Named("route").
		Complete(r)
}
