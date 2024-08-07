{{- if .Values.storage.enabled -}}
{{- $fullName := include "worterbuch.fullname" . -}}
{{- $size := .Values.storage.size -}}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ $fullName }}
spec:
  {{- with .Values.storage.className }}
  storageClassName:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: {{ $size }}
{{- end }}