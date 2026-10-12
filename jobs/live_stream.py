
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP", "kafka:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "flight-events"
)

CLICKHOUSE_URL = os.getenv(
    "CLICKHOUSE_JDBC_URL",
    "jdbc:clickhouse://clickhouse:8123/flights",
)

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv(
    "CLICKHOUSE_PASSWORD", "admin123"
)

CHECKPOINT = os.getenv(
    "CHECKPOINT_LOCATION",
    "/tmp/checkpoints/live_stream",
)

MAX_OFFSETS = int(
    os.getenv("MAX_OFFSETS_PER_TRIGGER", "5000")
)

TRIGGER_SECONDS = int(
    os.getenv("TRIGGER_SECONDS", "5")
)

STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")


spark = (
    SparkSession.builder
    .appName("live_stream")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("flight_date", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("dest", StringType(), True),
    StructField("dep_time_blk", StringType(), True),
    StructField("dep_delay", DoubleType(), True),
    StructField("arr_delay", DoubleType(), True),
    StructField("cancelled", IntegerType(), True),
    StructField("cancellation_code", StringType(), True),
    StructField("distance", DoubleType(), True),
])


raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load()
)


events = (
    raw.select(
        F.from_json(
            F.col("value").cast("string"),
            schema,
        ).alias("e")
    )
    .select("e.*")
)


def write_ch(df, table):
    (
        df.write
        .format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option(
            "driver",
            "com.clickhouse.jdbc.ClickHouseDriver",
        )
        .option("dbtable", table)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("batchsize", "5000")
        .option("numPartitions", "1")
        .mode("append")
        .save()
    )


def process_batch(batch_df, batch_id):
    batch_df.persist()

    try:
        record_count = batch_df.count()

        if record_count == 0:
            print(
                f"Batch {batch_id}: empty",
                flush=True,
            )
            return

        clean = (
            batch_df
            .withColumn(
                "airline",
                F.when(
                    F.col("airline").isNull()
                    | (F.trim(F.col("airline")) == ""),
                    F.lit("UNKNOWN"),
                ).otherwise(F.trim(F.col("airline"))),
            )
            .withColumn(
                "event_ts",
                F.to_timestamp(
                    "event_time",
                    "yyyy-MM-dd HH:mm:ss",
                ),
            )
            .withColumn(
                "flight_dt",
                F.to_date("flight_date"),
            )
        )

        # Reject records with invalid ingestion timestamps
        # or invalid flight dates.
        valid = clean.filter(
            F.col("event_ts").isNotNull()
            & F.col("flight_dt").isNotNull()
        )

        live = valid.select(
            F.col("event_ts").alias("event_time"),
            F.col("flight_dt").alias("flight_date"),
            "airline",
            "origin",
            "dest",
            F.concat_ws(
                "-",
                "origin",
                "dest",
            ).alias("route"),
            "dep_time_blk",
            "dep_delay",
            "arr_delay",
            F.coalesce(
                F.col("cancelled"),
                F.lit(0),
            ).cast("int").alias("cancelled"),
            F.coalesce(
                F.col("cancellation_code"),
                F.lit(""),
            ).alias("cancellation_code"),
            "distance",
            F.when(
                F.col("cancelled") == 1,
                F.lit("Cancelled"),
            )
            .when(
                F.col("dep_delay") >= 60,
                F.lit("Severe delay"),
            )
            .when(
                F.col("dep_delay") >= 15,
                F.lit("Delayed"),
            )
            .otherwise(
                F.lit("On time")
            ).alias("delay_category"),
        )

        # Per-batch airline statistics.
        stats = (
            valid
            .filter(F.col("airline") != "UNKNOWN")
            .groupBy("airline")
            .agg(
                F.count("*")
                .cast("int")
                .alias("flights"),

                F.sum(
                    F.when(
                        F.col("arr_delay") > 15,
                        1,
                    ).otherwise(0)
                ).cast("int").alias("delayed_flights"),

                F.sum(
                    F.when(
                        F.col("arr_delay").isNotNull(),
                        1,
                    ).otherwise(0)
                ).cast("int").alias("flights_with_arr_delay"),

                F.sum(
                    F.coalesce(
                        F.col("cancelled"),
                        F.lit(0),
                    )
                ).cast("int").alias("cancelled_flights"),

                F.coalesce(
                    F.sum("dep_delay"),
                    F.lit(0.0),
                ).alias("total_dep_delay"),
            )
            .withColumn(
                "batch_time",
                F.current_timestamp(),
            )
            .select(
                "batch_time",
                "airline",
                "flights",
                "delayed_flights",
                "flights_with_arr_delay",
                "cancelled_flights",
                "total_dep_delay",
            )
        )

        live_count = live.count()

        if live_count > 0:
            write_ch(live, "flights_live")

        stats_count = stats.count()

        if stats_count > 0:
            write_ch(stats, "live_airline_stats")

        print(
            f"Batch {batch_id}: "
            f"kafka_records={record_count}, "
            f"valid_flights={live_count}, "
            f"airline_stat_rows={stats_count}",
            flush=True,
        )

    except Exception as exc:
        print(
            f"ERROR processing batch {batch_id}: {exc}",
            flush=True,
        )
        raise

    finally:
        batch_df.unpersist()


query = (
    events.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(
        processingTime=f"{TRIGGER_SECONDS} seconds"
    )
    .start()
)

query.awaitTermination()
