FROM docker.io/bitnami/spark:latest

WORKDIR /app

ENV SPARK_MODE=master\
    SPARK_RPC_AUTHENTICATION_ENABLED=no\
    SPARK_RPC_ENCRYPTION_ENABLED=no\
    SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no\
    SPARK_SSL_ENABLED=no

COPY ./spark-streaming/streaming.py /app/

CMD /opt/bitnami/scripts/spark/run.sh & (sleep 30; spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --master spark://spark-master:7077 /app/streaming.py)