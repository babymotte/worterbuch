{{- range $nodeID, $config := $.Values.nodes }}
---
apiVersion: v1
kind: Service
metadata:
  name: worterbuch-{{ $nodeID }}-internal
  labels:
    {{- include "worterbuch.labels" $ | nindent 4 }}
spec:
  type: ClusterIP
  ports:
    - port: {{ $.Values.service.port.clusterSync }}
      targetPort: cluster-sync
      protocol: TCP
      name: cluster-sync
    - port: {{ $.Values.service.port.raft }}
      targetPort: raft
      protocol: UDP
      name: raft
  selector:
    app.kubernetes.io/name: {{ include "worterbuch.name" $ }}
    app.kubernetes.io/instance: worterbuch-{{ $nodeID }}
  publishNotReadyAddresses: true
---
apiVersion: v1
kind: Service
metadata:
  name: worterbuch-{{ $nodeID }}-stats
  labels:
    {{- include "worterbuch.labels" $ | nindent 4 }}
spec:
  type: {{ $.Values.service.type }}
  ports:
    - port: {{ $.Values.service.port.stats }}
      targetPort: stats
      protocol: TCP
      name: stats
      {{- if eq $.Values.service.type "NodePort" }}
      nodePort: {{ $config.statsNodePort }}
      {{ end }}
  selector:
    app.kubernetes.io/name: {{ include "worterbuch.name" $ }}
    app.kubernetes.io/instance: worterbuch-{{ $nodeID }}
  publishNotReadyAddresses: true
---
apiVersion: v1
kind: Service
metadata:
  name: worterbuch-{{ $nodeID }}-leader
  labels:
    {{- include "worterbuch.labels" $  | nindent 4 }}
spec:
  type: {{ $.Values.service.type }}
  ports:
    - port: {{ $.Values.service.port.http }}
      targetPort: http
      protocol: TCP
      name: http
      {{- if eq $.Values.service.type "NodePort" }}
      nodePort: {{ $config.httpNodePort }}
      {{ end }}
    - port: {{ $.Values.service.port.tcp }}
      targetPort: tcp
      protocol: TCP
      name: tcp
      {{- if eq $.Values.service.type "NodePort" }}
      nodePort: {{ $config.tcpNodePort }}
      {{ end }}
  selector:
    app.kubernetes.io/name: {{ include "worterbuch.name" $ }}
    app.kubernetes.io/instance: worterbuch-{{ $nodeID }}
  publishNotReadyAddresses: false
{{- end }}