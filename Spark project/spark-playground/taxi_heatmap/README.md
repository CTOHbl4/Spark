### Taxi heatmap

# Description

We take a file telem.json from [taxi simulator](https://gitlab.com/event-processing-msu/taxi-simulator). So it is better to study it there, but in short, at each timestamp we obtain position change of some taxis. Using this change we prepare data in pyspark and log the heatmap with python consumer.

The heatmaps may be seen in the logs of **python-consumer** container.

Use this project to understand, how to manade the data flow generator to file -> producer to Kafka -> Spark to Kafka -> consumer to console, as Spark is used here only to divide coordinates by a constant number, which is nothing.

# Usage

1) sudo sh run.sh -- create containers from docker compose and start the simulation.

2) docker compose down -v -- stop the simulation and delete the containers and volumes.

# Tools description

The used tools are: docker, kafka, spark structured streaming, sh scripting and python.

1) How kafka (KRaft) works (in short, as I understood):

The main task of Kafka is to collect messages, store them effectively and make them easily extractable.

Kafka consists of the following main parts: brokers and bootstrap servers, topics, partitions, producers and consumers.

- topic is a storing instance, where we send messages (message is \<(key), (value), ...\>). They may be split into partitions by kafka (for effectiveness of collection and retrieval). Partitions store messages of a specified topic only with certain "key" field values or by special parameters, so that one message is only in one partition.
- Bootstrap server is a broker, to which we firstly connect to extract all the necessary information about the kafka server (So, it is a broker and a "source" of information about the server).
- Broker controls its partitions of topics.
- Producer is a program which sends messages to partitions.
- Consumer is a program which collects messages from partitions.

So, imagine we have 2 brokers, 1 topic with 2 partitions, and the first broker is also a bootstrap server. If we do everything correctly, Kafka server automatically decides to balance the partitions among the brokers. the first partition is on the first broker and the second is on the second.

2) Spark Structured Streaming:

Here we are free to say that Spark is not used at all. All Spark does is just division, which is close to nothing.

The way Spark Structured Streaming is that we collect "micro batches" and do regular DataFrame operations with them.

3) Docker compose:

Docker compose might not also be fully used, as I only use its capabilities to create the containers (not start) and down them.

Things which may be added/changed will be described later.

# Possible changes

1) As in telem.json we get updates only for some of taxis, we should treat each new message as an event. That forces limitations.

The initial goal was to use something like: "**SELECT details.id, CAST(details.location.x/1000 AS int) AS X, CAST(details.location.y/1000 AS int) AS Y FROM taxi GROUP BY time, X, Y;**", but here at each timestamp we should obtain information about every taxi in a scene.

Thus, we may use the taxi generator, which is in a merge request of the [taxi simulator](https://gitlab.com/event-processing-msu/taxi-simulator).

2) Another possible option, that might work, is to study the update output mode with a **stateful** paradigm. All we need is
    - Save actual position for each taxi in memory,
    - Update positions as messages come,
    - With each update, dump (select count(taxi_id) from taxi group by x//NUM_COLUMNS, y//NUM_ROWS) to kafka, where taxi contains actual information about each taxi.

# Useful links

1) https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

2) https://kafka-python.readthedocs.io/en/master/

3) https://www.confluent.io/blog/apache-kafka-intro-how-kafka-works/
