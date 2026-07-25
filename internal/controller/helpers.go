package controller

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// wrapperspb_BoolValue is a convenience wrapper.
type wrapperspb_BoolValue = wrapperspb.BoolValue

// parseDuration parses a Kubernetes-style duration string (e.g., "60s", "300s", "24h")
// into a protobuf Duration.
func parseDuration(s string) (*durationpb.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return nil, err
	}
	return durationpb.New(d), nil
}

// mustStruct converts a map to a protobuf Struct, panicking on error.
func mustStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

func parseTLSCertNotAfter(secret *corev1.Secret) (time.Time, error) {
	data, ok := secret.Data["tls.crt"]
	if !ok || len(data) == 0 {
		return time.Time{}, fmt.Errorf("secret %s/%s has no tls.crt data", secret.Namespace, secret.Name)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return time.Time{}, fmt.Errorf("secret %s/%s tls.crt is not valid PEM", secret.Namespace, secret.Name)
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Time{}, fmt.Errorf("secret %s/%s tls.crt parse failed: %w", secret.Namespace, secret.Name, err)
	}

	return cert.NotAfter, nil
}

func certificateExpiryState(now, notAfter time.Time, warnBefore time.Duration) (metav1ConditionStatus string, reason string, message string) {
	if !now.Before(notAfter) {
		return "False", "CertificateExpired", fmt.Sprintf("Gateway TLS certificate expired at %s", notAfter.Format(time.RFC3339))
	}

	remaining := notAfter.Sub(now)
	if remaining <= warnBefore {
		return "False", "CertificateExpiring", fmt.Sprintf("Gateway TLS certificate expires in %s at %s", remaining.Round(time.Hour).String(), notAfter.Format(time.RFC3339))
	}

	return "True", "CertificateHealthy", fmt.Sprintf("Gateway TLS certificate valid until %s", notAfter.Format(time.RFC3339))
}
