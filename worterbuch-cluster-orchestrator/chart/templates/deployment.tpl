{{- range $nodeID, $config := $.Values.nodes }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worterbuch-{{ $nodeID }}
  labels:
    {{- include "worterbuch.labels" $ | nindent 4 }}
spec:
  {{- if not $.Values.autoscaling.enabled }}
  replicas: {{ $.Values.replicaCount }}
  {{- end }}
  selector:
    matchLabels:
      app.kubernetes.io/name: {{ include "worterbuch.name" $ }}
      app.kubernetes.io/instance: worterbuch-{{ $nodeID }}
  template:
    metadata:
      {{- with $.Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      labels:
        app.kubernetes.io/name: {{ include "worterbuch.name" $ }}
        app.kubernetes.io/instance: worterbuch-{{ $nodeID }}
    spec:
      {{- with $.Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      serviceAccountName: {{ include "worterbuch.serviceAccountName" $ }}
      securityContext:
        {{- toYaml $.Values.podSecurityContext | nindent 8 }}
      volumes:
        - name: worterbuch-cluster-config
          configMap:
            name: worterbuch-cluster-config
      {{- if $.Values.storage.data.enabled }}
        - name: worterbuch-{{ $nodeID }}-data
          persistentVolumeClaim:
            claimName: worterbuch-{{ $nodeID }}-data
      {{- end }}
        - name: worterbuch-{{ $nodeID }}-profiling
          persistentVolumeClaim:
            claimName: worterbuch-{{ $nodeID }}-profiling
      containers:
        - name: worterbuch-{{ $nodeID }}
          args: 
            - {{ $nodeID }}
          securityContext:
            {{- toYaml $.Values.securityContext | nindent 12 }}
          image: "{{ $.Values.image.repository }}:{{ $.Values.image.tag | default $.Chart.AppVersion }}"
          imagePullPolicy: {{ $.Values.image.pullPolicy }}
          env:
            - name: WORTERBUCH_LOG
              value: info,worterbuch_cluster_orchestrator::stats=warn
            - name: WORTERBUCH_TRACING
              value: info,worterbuch_cluster_orchestrator=debug,worterbuch_cluster_orchestrator::stats=warn,worterbuch=debug
            - name: WBCLUSTER_RAFT_PORT
              value: "{{ $.Values.service.port.raft }}"
            - name: WBCLUSTER_STATS_PORT
              value: "{{ $.Values.service.port.stats }}"
            - name: WBCLUSTER_HEARTBEAT_INTERVAL
              value: "{{ $.Values.leaderElection.heartbeat.interval }}"
            - name: WBCLUSTER_HEARTBEAT_MIN_TIMEOUT
              value: "{{ $.Values.leaderElection.heartbeat.timeout }}"
            - name: WORTERBUCH_PUBLIC_ADDRESS
              value: {{ (first $.Values.ingress.hosts).host }}
            - name: WORTERBUCH_TCP_BIND_ADDRESS
              value: 0.0.0.0
            - name: WORTERBUCH_WS_BIND_ADDRESS
              value: 0.0.0.0
            - name: WORTERBUCH_TCP_SERVER_PORT
              value: "{{ $.Values.service.port.tcp }}"
            - name: WORTERBUCH_WS_SERVER_PORT
              value: "{{ $.Values.service.port.http }}"
            - name: WORTERBUCH_KEEPALIVE_TIME
              value: "{{ $.Values.keepalive.time }}"
            - name: WORTERBUCH_KEEPALIVE_INTERVAL
              value: "{{ $.Values.keepalive.interval }}"
            - name: WORTERBUCH_KEEPALIVE_RETRIES
              value: "{{ $.Values.keepalive.retries }}"
            - name: WORTERBUCH_SEND_TIMEOUT
              value: "{{ $.Values.timeouts.send }}"
            - name: WORTERBUCH_SHUTDOWN_TIMEOUT
              value: "{{ $.Values.timeouts.shutdown }}"
            - name: WORTERBUCH_CHANNEL_BUFFER_SIZE
              value: "{{ $.Values.channelBufferSize }}"
            - name: WORTERBUCH_EXTENDED_MONITORING
              value: "{{ $.Values.extendedMonitoring }}"
            - name: WORTERBUCH_PERSISTENCE_INTERVAL
              value: "{{ $.Values.persistence.interval }}"
            - name: WORTERBUCH_PERSISTENCE_MODE
              value: "{{ $.Values.persistence.mode }}"
            {{- with $.Values.authToken }}
            - name: WORTERBUCH_AUTH_TOKEN
              value: "{{ . }}"
            {{- end }}
            {{- with $.Values.cors.allowedOrigins }}
            - name: WORTERBUCH_CORS_ALLOWED_ORIGINS
              value: "{{ . }}"
            {{- end }}
            - name: WORTERBUCH_SINGLE_THREADED
              value: "false"
            {{- with $.Values.telemetry.endpoint.grpc }}
            - name: WORTERBUCH_OPENTELEMETRY_ENDPOINT
              value: {{ . }}
            {{- end }}
            - name: MALLOC_CONF
              value: "thp:always,metadata_thp:always,prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:30,prof_gdump:false,prof_leak:true,prof_final:true,prof_prefix:/profiling/jeprof"
          ports:
            - name: http
              containerPort: {{ $.Values.service.port.http }}
              protocol: TCP
            - name: tcp
              containerPort: {{ $.Values.service.port.tcp }}
              protocol: TCP
            - name: cluster-sync
              containerPort: {{ $.Values.service.port.clusterSync }}
              protocol: TCP
            - name: raft
              containerPort: {{ $.Values.service.port.raft }}
              protocol: UDP
            - name: stats
              containerPort: {{ $.Values.service.port.stats }}
              protocol: TCP
          {{- with $.Values.livenessProbe }}
          livenessProbe:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with $.Values.readinessProbe }}
          readinessProbe:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          resources:
            {{- toYaml $.Values.resources | nindent 12 }}
          volumeMounts:
          {{- if $.Values.storage.enabled }}
            - mountPath: "/data"
              name: worterbuch-{{ $nodeID }}-data
          {{- end }}
            - mountPath: "/cfg"
              name: worterbuch-cluster-config
              readOnly: true
            - mountPath: "/profiling"
              name: worterbuch-{{ $nodeID }}-profiling
      {{- with $.Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $.Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $.Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}

{{- end }}