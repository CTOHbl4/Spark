#!/bin/bash
sudo docker compose create 
docker start kafka1 spark-master kafka2
sleep 5
docker start spark-worker-1 spark-worker-2

# wait for services to start.
sleep 3
docker start python-producer-taxi python-consumer
docker exec -d spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/streaming.py
