from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time


SHUFFLE_PARTITIONS = 4
WATERMARK = "20 seconds"
TAXIS = 100


def main():
    # SparkSession.
    spark = SparkSession.builder\
        .master("spark://spark-master:7077")\
        .appName("Spark Kafka Streaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions",
                SHUFFLE_PARTITIONS) \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    # if we need several queries to be executed simultaneously => FAIR resource distribution.
    # spark.sparkContext.setLocalProperty("spark.scheduler.mode", "FAIR")

    # Read topic with coordinates.
    df_taxi = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092") \
        .option("subscribe", "test-topic") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", TAXIS) \
        .load().selectExpr("CAST(value AS STRING)")

    # Create schema from json.
    # Do not call a column as "timestamp" as it might conflict with special words.
    json_string = '{"details":{"location":{"x":0.0,"y":0.0},"id":1,"state":"str"},"ts":0.0,"event_type":"str"}'
    json_schema = F.schema_of_json(json_string)
    df_taxi = df_taxi.withColumn('value', F.from_json(F.col('value'),
                                                      json_schema)) \
        .select(F.col("value.*"))

    df_taxi = df_taxi.withColumn("wtime", df_taxi.select("ts")[0].cast("timestamp")) \
        .withWatermark("wtime", WATERMARK)

    # Create Temporary View for sql operations.
    df_taxi.createOrReplaceTempView("_taxi")
    spark.sql(f"""
    create or replace temporary view taxi as (
    select details.id as id, details.location.x as x, details.location.y as y from _taxi
    )
    """)

    # Get the stream for output as a result of sql operation.
    StreamIdle = spark.sql("""
                    select
                           id,
                           cast(max(x)/1000 as int) as X,
                           cast(max(y)/1000 as int) as Y
                           from taxi
                           group by id
    """)

    # Write output stream to kafka.
    # Complete output mode guarantees one line for each taxi.
    StreamIdle.selectExpr("cast(id as string) as key", "concat(cast(X as string), \",\", cast(Y as string)) as value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092") \
        .option("topic", "semioutput-topic") \
        .option("checkpointLocation", "/tmp/checkpoint1") \
        .outputMode("complete") \
        .start()

    # Wait for the semioutput-topic.
    time.sleep(20)

    df_res = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092") \
        .option("subscribe", "semioutput-topic") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", TAXIS) \
        .load().selectExpr("CAST(key AS STRING) as id", "split(CAST(value AS STRING), ',') as splitted", "offset") \

    df_res = df_res.selectExpr("id", "cast(splitted[0] as int) as x", "cast(splitted[1] as int) as y", f"cast(offset/{TAXIS} as int) as offset")
    df_res = df_res.selectExpr("id", "x", "y", "offset", "to_timestamp(offset) as wtime") \
        .withWatermark("wtime", WATERMARK)

    df_res.createOrReplaceTempView("ids")

    StreamRes = spark.sql("""
                    select count(*) as num,
                           X,
                           Y,
                           wtime
                           from ids
                           group by x, y, wtime
    """)

    StreamRes.selectExpr("cast(num as string) as key", "concat(cast(X as string), \",\", cast(Y as string)) as value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka2:9093") \
        .option("topic", "output-topic") \
        .option("checkpointLocation", "/tmp/checkpoint2") \
        .outputMode("append") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
