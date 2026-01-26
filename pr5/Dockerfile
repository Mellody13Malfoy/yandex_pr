FROM python:3.9-alpine
WORKDIR /app
RUN pip install prometheus-client requests
COPY kafka_connect_exporter.py .
EXPOSE 8084
CMD ["python", "kafka_connect_exporter.py"]
