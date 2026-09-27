{{- define "dn.name" -}}
{{- printf "%s-nix" .Release.Name | trunc 45 | trimSuffix "-" -}}
{{- end -}}
{{- define "dn.secret" -}}
{{- default (include "dn.name" .) .Values.auth.existingSecret -}}
{{- end -}}
{{- define "dn.labels" -}}
app.kubernetes.io/name: distributed-nix
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}
