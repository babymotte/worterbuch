{{- range $nodeID, $config := $.Values.nodes }}
{{- if $.Values.storage.data.enabled }}
---
{{- $size := $.Values.storage.data.size -}}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: worterbuch-{{ $nodeID }}-data
spec:
  {{- with $.Values.storage.data.className }}
  storageClassName:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  accessModes:
    # - ReadWriteMany
    - ReadWriteOnce
  resources:
    requests:
      storage: {{ $size }}
{{- end }}
---
{{- $size := $.Values.storage.profiling.size -}}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: worterbuch-{{ $nodeID }}-profiling
spec:
  {{- with $.Values.storage.profiling.className }}
  storageClassName:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  accessModes:
    # - ReadWriteMany
    - ReadWriteOnce
  resources:
    requests:
      storage: {{ $size }}
{{- end }}