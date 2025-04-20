from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# Important parameters.
# For example, if watermark is too small for a specified
# number of partitions (there are more partitions then there is data in
# a watermark) partitions will require the absent data.
# More workers can handle more partitions.
WATERMARK = "100 seconds"
SHUFFLE_PARTITIONS = "5"


def main():
    # Create SparkSession.
    spark = SparkSession.builder\
        .master("spark://spark-master:7077")\
        .appName("Spark Kafka Streaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions",
                SHUFFLE_PARTITIONS) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    # If we need several queries to be executed simultaneously =>
    # FAIR resource distribution.
    # spark.sparkContext.setLocalProperty("spark.scheduler.mode", "FAIR")

    # Create schema from json.
    json_string = '{"client_id":1,"timestamp":"2024-01-24","spendings":1684.0}'
    json_schema = F.schema_of_json(json_string)

    # Create streams from topics.
    # Watermarks are based on "timestamp" from stream.
    dfs = []
    for topic in ("entertainment", "fuel", "shopping", "taxes"):
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load().selectExpr("CAST(value AS STRING)")
        df = df.withColumn('value', F.from_json(F.col('value'), json_schema)) \
            .select(F.col("value.*"))
        ts = df.select("timestamp")[0]
        df = df.selectExpr("client_id",
                           "timestamp",
                           f"spendings as {topic}_spendings") \
            .withColumn("wtime", ts.cast("timestamp")) \
            .withWatermark("wtime", WATERMARK)

        dfs.append(df)

    # Join spendings.
    joined_df = dfs[0]
    for df in dfs[1:]:
        joined_df = joined_df.join(df, ["client_id", "wtime"], "inner")

    # Temporary view for sql.
    joined_df.createOrReplaceTempView("spendings")

    # Get the stream for output as a result of sql operation.
    # Also aggregation for total spendings.
    StreamDaySum = spark.sql(
        """
        select
            sum(taxes_spendings + shopping_spendings + fuel_spendings +
                                entertainment_spendings) as total_spendings,
            wtime
        from spendings
        group by wtime;
        """
                             )

    # Write output stream to another kafka bootstrap server.
    StreamDaySum.selectExpr("cast(total_spendings as string) as value",
                            "cast(wtime as string) as key") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka_output:9093") \
        .option("topic", "output") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .outputMode("append") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
