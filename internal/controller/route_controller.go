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
	"strconv"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	networkingv1 "istio.io/client-go/pkg/apis/networking/v1"
	istiov1alpha3 "istio.io/client-go/pkg/apis/networking/v1alpha3"
	istionetv1 "istio.io/api/networking/v1"
	istionetv1alpha3 "istio.io/api/networking/v1alpha3"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	platformv1alpha1 "github.com/istio-gateway-operator/route-operator/api/v1alpha1"
)

const (
	routeFinalizer = "istio-gateway-api-operator.io/finalizer"
	managedByLabel = "istio-gateway-api-operator.io/managed-by"
	routeNameLabel = "istio-gateway-api-operator.io/route-name"
)

// RouteReconciler reconciles a Route object
type RouteReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=istio-gateway-api-operator.io,resources=routes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=istio-gateway-api-operator.io,resources=routes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=istio-gateway-api-operator.io,resources=routes/finalizers,verbs=update
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=virtualservices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=destinationrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=envoyfilters,verbs=get;list;watch;create;update;patch;delete

func (r *RouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the Route instance
	route := &platformv1alpha1.Route{}
	if err := r.Get(ctx, req.NamespacedName, route); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion
	if !route.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(route, routeFinalizer) {
			if err := r.cleanupOwnedResources(ctx, route); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(route, routeFinalizer)
			if err := r.Update(ctx, route); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(route, routeFinalizer) {
		controllerutil.AddFinalizer(route, routeFinalizer)
		if err := r.Update(ctx, route); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Reconcile child resources
	managedCount := 0
	var reconcileErr error

	// 1. HTTPRoutes (one per rule)
	for i, rule := range route.Spec.Rules {
		if err := r.reconcileHTTPRoute(ctx, route, i, rule); err != nil {
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

	// 3. VirtualService (if timeout or CORS)
	if route.Spec.Timeout != "" || route.Spec.CORS != nil {
		if err := r.reconcileVirtualService(ctx, route); err != nil {
			log.Error(err, "Failed to reconcile VirtualService")
			reconcileErr = err
		} else {
			managedCount++
		}
	}

	// 4. DestinationRules (for HTTPS backends)
	drIndex := 0
	for _, rule := range route.Spec.Rules {
		protocol := rule.Backend.Protocol
		if protocol == "" {
			protocol = "HTTP"
		}
		if protocol == "HTTPS" {
			if err := r.reconcileDestinationRule(ctx, route, drIndex, rule); err != nil {
				log.Error(err, "Failed to reconcile DestinationRule")
				reconcileErr = err
			} else {
				managedCount++
			}
			drIndex++
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

	// Update status
	route.Status.ManagedResources = managedCount
	if reconcileErr != nil {
		r.setCondition(route, string(platformv1alpha1.RouteConditionSynced), metav1.ConditionFalse, "ReconcileError", reconcileErr.Error())
		r.setCondition(route, string(platformv1alpha1.RouteConditionReady), metav1.ConditionFalse, "NotReady", "Some resources failed to reconcile")
	} else {
		r.setCondition(route, string(platformv1alpha1.RouteConditionSynced), metav1.ConditionTrue, "ReconcileSuccess", "All resources synced")
		r.setCondition(route, string(platformv1alpha1.RouteConditionReady), metav1.ConditionTrue, "Available", "All resources available")
	}

	if err := r.Status().Update(ctx, route); err != nil {
		log.Error(err, "Failed to update Route status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, reconcileErr
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

func (r *RouteReconciler) reconcileHTTPRoute(ctx context.Context, route *platformv1alpha1.Route, index int, rule platformv1alpha1.RouteRule) error {
	gwName, gwNs := r.gatewayRef(route)
	ns := gatewayv1.Namespace(gwNs)
	sectionName := gatewayv1.SectionName("https")
	path := rule.Path
	if path == "" {
		path = "/"
	}
	pathType := gatewayv1.PathMatchPathPrefix
	port := gatewayv1.PortNumber(rule.Backend.ServicePort)

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
				BackendRefs: []gatewayv1.HTTPBackendRef{{
					BackendRef: gatewayv1.BackendRef{
						BackendObjectReference: gatewayv1.BackendObjectReference{
							Name: gatewayv1.ObjectName(rule.Backend.ServiceName),
							Port: &port,
						},
					},
				}},
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

func (r *RouteReconciler) reconcileVirtualService(ctx context.Context, route *platformv1alpha1.Route) error {
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
			Route: []*istionetv1.HTTPRouteDestination{{
				Destination: &istionetv1.Destination{
					Host: fmt.Sprintf("%s.%s.svc.cluster.local", rule.Backend.ServiceName, route.Namespace),
					Port: &istionetv1.PortSelector{Number: uint32(rule.Backend.ServicePort)},
				},
			}},
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

func (r *RouteReconciler) reconcileDestinationRule(ctx context.Context, route *platformv1alpha1.Route, index int, rule platformv1alpha1.RouteRule) error {
	_, gwNs := r.gatewayRef(route)

	dr := &networkingv1.DestinationRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-tls-%d", route.Name, index),
			Namespace: gwNs,
			Labels:    r.managedLabels(route),
		},
	}
	dr.Spec.Host = fmt.Sprintf("%s.%s.svc.cluster.local", rule.Backend.ServiceName, route.Namespace)
	dr.Spec.TrafficPolicy = &istionetv1.TrafficPolicy{
		PortLevelSettings: []*istionetv1.TrafficPolicy_PortTrafficPolicy{{
			Port: &istionetv1.PortSelector{Number: uint32(rule.Backend.ServicePort)},
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

	return nil
}

func (r *RouteReconciler) managedLabels(route *platformv1alpha1.Route) map[string]string {
	return map[string]string{
		managedByLabel: "route-operator",
		routeNameLabel: route.Name,
	}
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
		Named("route").
		Complete(r)
}
