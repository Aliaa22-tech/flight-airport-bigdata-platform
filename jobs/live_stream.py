from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

spark = (
    SparkSession.builder.appName("live_stream")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_time", StringType()),
    StructField("flight_date", StringType()),
    StructField("airline", StringType()),
    StructField("origin", StringType()),
    StructField("dest", StringType()),
    StructField("dep_time_blk", StringType()),
    StructField("dep_delay", DoubleType()),
    StructField("arr_delay", DoubleType()),
    StructField("cancelled", IntegerType()),
    StructField("cancellation_code", StringType()),
    StructField("distance", DoubleType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "flight-events")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 2000)
    .load()
)

events = raw.select(
    F.from_json(F.col("value").cast("string"), schema).alias("e")
).select("e.*")


def write_ch(df, table):
    (
        df.write.format("jdbc")
        .option("url", "jdbc:clickhouse://clickhouse:8123/flights")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", table)
        .option("user", "admin")
        .option("password", "admin123")
        .option("batchsize", 10000)
        .mode("append")
        .save()
    )


def process_batch(batch_df, batch_id):
    batch_df.persist()
    n = batch_df.count()
    if n == 0:
        batch_df.unpersist()
        return

    live = batch_df.select(
        F.to_timestamp("event_time").alias("event_time"),
        F.to_date("flight_date").alias("flight_date"),
        "airline",
        "origin",
        "dest",
        F.concat_ws("-", "origin", "dest").alias("route"),
        "dep_time_blk",
        "dep_delay",
        "arr_delay",
        F.coalesce(F.col("cancelled"), F.lit(0)).alias("cancelled"),
        F.coalesce(F.col("cancellation_code"), F.lit("")).alias("cancellation_code"),
        "distance",
        F.when(F.col("cancelled") == 1, "Cancelled")
        .when(F.col("dep_delay") >= 60, "Severe delay")
        .when(F.col("dep_delay") >= 15, "Delayed")
        .otherwise("On time")
        .alias("delay_category"),
    )
    write_ch(live, "flights_live")

    stats = (
        batch_df.groupBy("airline")
        .agg(
            F.count("*").cast("int").alias("flights"),
            F.sum(F.when(F.col("dep_delay") >= 15, 1).otherwise(0)).cast("int").alias("delayed_flights"),
            F.sum(F.coalesce(F.col("cancelled"), F.lit(0))).cast("int").alias("cancelled_flights"),
            F.coalesce(F.sum("dep_delay"), F.lit(0.0)).alias("total_dep_delay"),
        )
        .withColumn("batch_time", F.current_timestamp())
        .select("batch_time", "airline", "flights", "delayed_flights", "cancelled_flights", "total_dep_delay")
    )
    write_ch(stats, "live_airline_stats")

    print("batch", batch_id, "->", n, "events written", flush=True)
    batch_df.unpersist()


query = (
    events.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/checkpoints/live_stream")
    .trigger(processingTime="5 seconds")
    .start()
)
query.awaitTermination()