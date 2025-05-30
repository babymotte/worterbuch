{{- if $.Values.ingress.enabled -}}
{{- range $nodeID, $config := $.Values.nodes }}
---
{{- $svcPort := $.Values.service.port.http -}}
{{- if and $.Values.ingress.className (not (semverCompare ">=1.18-0" $.Capabilities.KubeVersion.GitVersion)) }}
  {{- if not (hasKey $.Values.ingress.annotations "kubernetes.io/ingress.class") }}
  {{- $_ := set $.Values.ingress.annotations "kubernetes.io/ingress.class" $.Values.ingress.className}}
  {{- end }}
{{- end }}
{{- if semverCompare ">=1.19-0" $.Capabilities.KubeVersion.GitVersion -}}
apiVersion: networking.k8s.io/v1
{{- else if semverCompare ">=1.14-0" $.Capabilities.KubeVersion.GitVersion -}}
apiVersion: networking.k8s.io/v1beta1
{{- else -}}
apiVersion: extensions/v1beta1
{{- end }}
kind: Ingress
metadata:
  name: worterbuch-{{ $nodeID }}
  labels:
    {{- include "worterbuch.labels" $ | nindent 4 }}
  {{- with $.Values.ingress.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  {{- if and $.Values.ingress.className (semverCompare ">=1.18-0" .Capabilities.KubeVersion.GitVersion) }}
  ingressClassName: {{ $.Values.ingress.className }}
  {{- end }}
  {{- if $.Values.ingress.tls }}
  tls:
    {{- range $.Values.ingress.tls }}
    - hosts:
        {{- range .hosts }}
        - {{ . | quote }}
        {{- end }}
      secretName: {{ .secretName }}
    {{- end }}
  {{- end }}
  rules:
    {{- range $.Values.ingress.hosts }}
    - host: {{ .host | quote }}
      http:
        paths:
          {{- range .paths }}
          - path: {{ .path }}
            {{- if and .pathType (semverCompare ">=1.18-0" $.Capabilities.KubeVersion.GitVersion) }}
            pathType: {{ .pathType }}
            {{- end }}
            backend:
              {{- if semverCompare ">=1.19-0" $.Capabilities.KubeVersion.GitVersion }}
              service:
                name: worterbuch-{{ $nodeID }}-leader
                port:
                  number: {{ $svcPort }}
              {{- else }}
              serviceName: worterbuch-{{ $nodeID }}-leader
              servicePort: {{ $svcPort }}
              {{- end }}
          {{- end }}
    {{- end }}
---
{{- $svcPort := $.Values.service.port.http -}}
{{- if and $.Values.ingress.className (not (semverCompare ">=1.18-0" $.Capabilities.KubeVersion.GitVersion)) }}
  {{- if not (hasKey $.Values.ingress.annotations "kubernetes.io/ingress.class") }}
  {{- $_ := set $.Values.ingress.annotations "kubernetes.io/ingress.class" $.Values.ingress.className}}
  {{- end }}
{{- end }}
{{- if semverCompare ">=1.19-0" $.Capabilities.KubeVersion.GitVersion -}}
apiVersion: networking.k8s.io/v1
{{- else if semverCompare ">=1.14-0" $.Capabilities.KubeVersion.GitVersion -}}
apiVersion: networking.k8s.io/v1beta1
{{- else -}}
apiVersion: extensions/v1beta1
{{- end }}
kind: Ingress
metadata:
  name: worterbuch-cluster-orchestrator-stats-{{ $nodeID }}
  labels:
    {{- include "worterbuch.labels" $ | nindent 4 }}
  {{- with $.Values.ingress.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  {{- if and $.Values.ingress.className (semverCompare ">=1.18-0" .Capabilities.KubeVersion.GitVersion) }}
  ingressClassName: {{ $.Values.ingress.className }}
  {{- end }}
  {{- if $.Values.ingress.tls }}
  tls:
    {{- range $.Values.ingress.tls }}
    - hosts:
        {{- range .hosts }}
        - {{ . | quote }}
        {{- end }}
      secretName: {{ .secretName }}
    {{- end }}
  {{- end }}
  rules:
    {{- range $.Values.ingress.stats.hosts }}
    - host: {{ .host | quote }}
      http:
        paths:
          {{- range .paths }}
          - path: {{ .path }}
            {{- if and .pathType (semverCompare ">=1.18-0" $.Capabilities.KubeVersion.GitVersion) }}
            pathType: {{ .pathType }}
            {{- end }}
            backend:
              {{- if semverCompare ">=1.19-0" $.Capabilities.KubeVersion.GitVersion }}
              service:
                name: worterbuch-{{ $nodeID }}-stats
                port:
                  number: {{ $svcPort }}
              {{- else }}
              serviceName: worterbuch-{{ $nodeID }}-stats
              servicePort: {{ $svcPort }}
              {{- end }}
          {{- end }}
    {{- end }}
{{- end }}
{{- end }}