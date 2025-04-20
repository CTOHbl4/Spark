#!/bin/bash

# Use correct stop in run.sh with deleting the python-consumer container.
variable=$(docker ps -q -f name="python-consumer")
while [ ! $variable ]
do
    sleep 0.2
    variable=$(docker ps -q -f name="python-consumer")
done
sleep 1
docker stop python-consumer

variable=$((docker ps -q -f name="kafka1") || (docker ps -q -f name="kafka2") ||
        (docker ps -q -f name="kafka_output"))
while [ $variable ]
do
    sleep 0.2
    variable=$((docker ps -q -f name="kafka1") || (docker ps -q -f name="kafka2") ||
        (docker ps -q -f name="kafka_output"))
done

sudo pkill -TERM sh run.sh
sudo pkill -TERM sh run_once.sh
docker compose -f docker-compose-spark.yml down
rm .env