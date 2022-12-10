apiVersion: v1
kind: Service
metadata:
  name: {{ include "chart.fullname" . }}
  labels:
    {{- include "chart.labels" . | nindent 4 }}
spec:
  type: {{ .Values.service.type }}
  ports:
    - port: {{ .Values.service.port }} 
      targetPort: tcp
      protocol: TCP
      name: tcp
      {{- with .Values.service.nodePort }}
      nodePort:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    - port: {{ .Values.service.httpPort }}
      targetPort: http
      protocol: TCP
      name: http
  selector:
    {{- include "chart.selectorLabels" . | nindent 4 }}
