{{/*
Expand the name of the chart.
*/}}
{{- define "igro.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "igro.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "igro.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "igro.labels" -}}
helm.sh/chart: {{ include "igro.chart" . }}
{{ include "igro.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "igro.selectorLabels" -}}
app.kubernetes.io/name: {{ include "igro.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
control-plane: controller-manager
{{- end }}

{{/*
Service account name
*/}}
{{- define "igro.serviceAccountName" -}}
{{- include "igro.fullname" . }}
{{- end }}

{{/*
Container image
*/}}
{{- define "igro.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Webhook certificate secret name
*/}}
{{- define "igro.webhookCertSecret" -}}
{{- printf "%s-webhook-cert" (include "igro.fullname" .) }}
{{- end }}

{{/*
Webhook service name
*/}}
{{- define "igro.webhookServiceName" -}}
{{- printf "%s-webhook" (include "igro.fullname" .) }}
{{- end }}
