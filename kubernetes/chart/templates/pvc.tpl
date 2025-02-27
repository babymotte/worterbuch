{{- range $nodeID, $config := $.Values.nodes }}
---
{{- $size := $.Values.storage.size -}}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: worterbuch-{{ $nodeID }}
spec:
  {{- with $.Values.storage.className }}
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