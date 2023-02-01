apiVersion: v1
kind: Service
metadata:
  name: {{ include "worterbuch.fullname" . }}
  labels:
    {{- include "worterbuch.labels" . | nindent 4 }}
spec:
  type: {{ .Values.service.type }}
  ports:
    - port: {{ .Values.service.port }}
      targetPort: http
      protocol: TCP
      name: http
  selector:
    {{- include "worterbuch.selectorLabels" . | nindent 4 }}
