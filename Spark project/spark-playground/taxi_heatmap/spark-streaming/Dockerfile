FROM docker.io/bitnami/spark:latest

WORKDIR /app

ENV SPARK_MODE=master\
    SPARK_RPC_AUTHENTICATION_ENABLED=no\
    SPARK_RPC_ENCRYPTION_ENABLED=no\
    SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no\
    SPARK_SSL_ENABLED=no

COPY ./spark-streaming/streaming.py /app/
