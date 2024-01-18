apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "worterbuch.fullname" . }}
  labels:
    {{- include "worterbuch.labels" . | nindent 4 }}
spec:
  {{- if not .Values.autoscaling.enabled }}
  replicas: {{ .Values.replicaCount }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "worterbuch.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      {{- with .Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      labels:
        {{- include "worterbuch.selectorLabels" . | nindent 8 }}
    spec:
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      serviceAccountName: {{ include "worterbuch.serviceAccountName" . }}
      securityContext:
        {{- toYaml .Values.podSecurityContext | nindent 8 }}
      {{- if .Values.storage.enabled }}
      volumes:
        - name: {{ include "worterbuch.fullname" . }}
          persistentVolumeClaim:
            claimName: {{ include "worterbuch.fullname" . }}
      {{- end }}
      containers:
        - name: {{ .Chart.Name }}
          securityContext:
            {{- toYaml .Values.securityContext | nindent 12 }}
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          env:          
            - name: WORTERBUCH_PUBLIC_ADDRESS
              value: {{ (first .Values.ingress.hosts).host }}
            - name: WORTERBUCH_WS_SERVER_PORT
              value: "{{ .Values.service.port }}"
            - name: WORTERBUCH_TCP_SERVER_PORT
              value: "{{ .Values.service.tcpport }}"
            - name: WORTERBUCH_WS_BIND_ADDRESS
              value: "0.0.0.0"
            - name: WORTERBUCH_TCP_BIND_ADDRESS
              value: "0.0.0.0"
          ports:
            - name: http
              containerPort: {{ .Values.service.port }}
              protocol: TCP
            - name: tcp
              containerPort: {{ .Values.service.tcpport }}
              protocol: TCP
          startupProbe:
            httpGet:
              path: /api/v1/get/%24SYS/uptime
              port: http
          livenessProbe:
            httpGet:
              path: /api/v1/get/%24SYS/uptime
              port: http
          readinessProbe:
            httpGet:
              path: /api/v1/get/%24SYS/uptime
              port: http
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
          {{- if .Values.storage.enabled }}
          volumeMounts:
            - mountPath: "/data"
              name: {{ include "worterbuch.fullname" . }}
          {{- end }}
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
