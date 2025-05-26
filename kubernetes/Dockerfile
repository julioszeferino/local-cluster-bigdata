FROM docker.io/bitnami/spark:3.5

USER root

RUN mkdir -p /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir delta-spark

COPY jars /opt/bitnami/spark/jars
COPY app /app/

WORKDIR /app
