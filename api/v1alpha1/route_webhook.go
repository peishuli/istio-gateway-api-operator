package v1alpha1

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const (
	defaultGatewayName      = "istio-gateway"
	defaultGatewayNamespace = "istio-system"
)

var routeWebhookClient client.Client

// +kubebuilder:webhook:path=/mutate-istio-gateway-api-operator-io-v1alpha1-route,mutating=true,failurePolicy=fail,sideEffects=None,groups=istio-gateway-api-operator.io,resources=routes,verbs=create;update,versions=v1alpha1,name=mroute.kb.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-istio-gateway-api-operator-io-v1alpha1-route,mutating=false,failurePolicy=fail,sideEffects=None,groups=istio-gateway-api-operator.io,resources=routes,verbs=create;update,versions=v1alpha1,name=vroute.kb.io,admissionReviewVersions=v1

func SetupRouteWebhookWithManager(mgr ctrl.Manager) error {
	routeWebhookClient = mgr.GetClient()
	return ctrl.NewWebhookManagedBy(mgr, &Route{}).
		WithDefaulter(&routeDefaulter{}).
		WithValidator(&routeValidator{}).
		Complete()
}

type routeDefaulter struct{}

func (d *routeDefaulter) Default(_ context.Context, obj *Route) error {
	if obj.Spec.Gateway == nil {
		obj.Spec.Gateway = &RouteGateway{}
	}
	if obj.Spec.Gateway.Name == "" {
		obj.Spec.Gateway.Name = defaultGatewayName
	}
	if obj.Spec.Gateway.Namespace == "" {
		obj.Spec.Gateway.Namespace = defaultGatewayNamespace
	}
	return nil
}

type routeValidator struct{}

func (v *routeValidator) ValidateCreate(ctx context.Context, obj *Route) (admission.Warnings, error) {
	return nil, validateRoute(ctx, obj)
}

func (v *routeValidator) ValidateUpdate(ctx context.Context, _ *Route, newObj *Route) (admission.Warnings, error) {
	return nil, validateRoute(ctx, newObj)
}

func (v *routeValidator) ValidateDelete(_ context.Context, _ *Route) (admission.Warnings, error) {
	return nil, nil
}

func validateRoute(ctx context.Context, route *Route) error {
	if routeWebhookClient == nil {
		return fmt.Errorf("route webhook client is not initialized")
	}

	if err := validateGatewayHostnames(ctx, route); err != nil {
		return err
	}
	if err := validateRouteConflicts(ctx, route); err != nil {
		return err
	}
	return nil
}

func validateGatewayHostnames(ctx context.Context, route *Route) error {
	gwName, gwNs := gatewayRefFromSpec(route.Spec.Gateway)

	gw := &gatewayv1.Gateway{}
	if err := routeWebhookClient.Get(ctx, types.NamespacedName{Name: gwName, Namespace: gwNs}, gw); err != nil {
		if apierrors.IsNotFound(err) {
			return apierrors.NewInvalid(
				GroupVersion.WithKind("Route").GroupKind(),
				route.Name,
				field.ErrorList{field.NotFound(field.NewPath("spec", "gateway"), fmt.Sprintf("%s/%s", gwNs, gwName))},
			)
		}
		return err
	}

	allowedPatterns := gatewayHostPatterns(gw)
	if len(allowedPatterns) == 0 {
		return nil
	}

	var allErrs field.ErrorList
	for i, rule := range route.Spec.Rules {
		if !hostMatchesAnyPattern(rule.Host, allowedPatterns) {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "rules").Index(i).Child("host"),
				rule.Host,
				fmt.Sprintf("host must match one of gateway listener hostnames: %s", strings.Join(allowedPatterns, ", ")),
			))
		}
	}

	if len(allErrs) > 0 {
		return apierrors.NewInvalid(GroupVersion.WithKind("Route").GroupKind(), route.Name, allErrs)
	}
	return nil
}

func validateRouteConflicts(ctx context.Context, route *Route) error {
	allRoutes := &RouteList{}
	if err := routeWebhookClient.List(ctx, allRoutes); err != nil {
		return err
	}

	currentRules := make(map[routeRuleIdentity]struct{})
	for _, rule := range route.Spec.Rules {
		currentRules[buildRuleIdentity(route, rule)] = struct{}{}
	}

	var allErrs field.ErrorList
	for i := range allRoutes.Items {
		other := &allRoutes.Items[i]
		if other.Namespace == route.Namespace && other.Name == route.Name {
			continue
		}
		if !other.DeletionTimestamp.IsZero() {
			continue
		}

		for _, otherRule := range other.Spec.Rules {
			id := buildRuleIdentity(other, otherRule)
			if _, exists := currentRules[id]; exists {
				allErrs = append(allErrs, field.Forbidden(
					field.NewPath("spec", "rules"),
					fmt.Sprintf("conflicts with existing Route %s/%s for gateway %s/%s host=%s path=%s", other.Namespace, other.Name, id.GatewayNamespace, id.GatewayName, id.Host, id.Path),
				))
				break
			}
		}
	}

	if len(allErrs) > 0 {
		return apierrors.NewInvalid(GroupVersion.WithKind("Route").GroupKind(), route.Name, allErrs)
	}
	return nil
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

func buildRuleIdentity(route *Route, rule RouteRule) routeRuleIdentity {
	gwName, gwNs := gatewayRefFromSpec(route.Spec.Gateway)
	return routeRuleIdentity{
		GatewayNamespace: gwNs,
		GatewayName:      gwName,
		Host:             rule.Host,
		Path:             normalizeRoutePath(rule.Path),
	}
}

func gatewayRefFromSpec(gw *RouteGateway) (string, string) {
	name := defaultGatewayName
	ns := defaultGatewayNamespace
	if gw != nil {
		if gw.Name != "" {
			name = gw.Name
		}
		if gw.Namespace != "" {
			ns = gw.Namespace
		}
	}
	return name, ns
}

func gatewayHostPatterns(gw *gatewayv1.Gateway) []string {
	seen := make(map[string]struct{})
	patterns := make([]string, 0)
	for _, listener := range gw.Spec.Listeners {
		if listener.Hostname == nil || string(*listener.Hostname) == "" {
			return nil
		}
		h := string(*listener.Hostname)
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		patterns = append(patterns, h)
	}
	return patterns
}

func hostMatchesAnyPattern(host string, patterns []string) bool {
	for _, pattern := range patterns {
		if hostMatchesPattern(host, pattern) {
			return true
		}
	}
	return false
}

func hostMatchesPattern(host, pattern string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		suffix := strings.TrimPrefix(pattern, "*.")
		if !strings.HasSuffix(host, "."+suffix) {
			return false
		}
		return host != suffix
	}
	return host == pattern
}

var _ admission.Defaulter[*Route] = &routeDefaulter{}
var _ admission.Validator[*Route] = &routeValidator{}
