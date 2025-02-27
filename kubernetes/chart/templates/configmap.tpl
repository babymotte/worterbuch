apiVersion: v1
kind: ConfigMap
metadata:
  name: worterbuch-cluster-config
data:
  config.yaml: |
    nodes:
      {{- range $nodeID, $config := $.Values.nodes }}
      - nodeId: {{ $nodeID }}
        address: worterbuch-{{ $nodeID }}-internal.{{ $.Release.Namespace }}.svc.cluster.local
        raftPort: {{ $.Values.service.port.raft }}
        syncPort: {{ $.Values.service.port.clusterSync }}
      {{- end }}
