from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

spark = SparkSession.builder.appName("analytics_job").getOrCreate()

URL = "jdbc:clickhouse://clickhouse:8123/flights"
OPTS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "admin",
    "password": "admin123",
}

# 1) Read the cleaned flights table from ClickHouse
raw = (spark.read.format("jdbc").options(**OPTS)
       .option("url", URL)
       .option("dbtable", "flights")
       .option("partitionColumn", "Month")
       .option("lowerBound", 1)
       .option("upperBound", 12)
       .option("numPartitions", 4)
       .load())

# 2) Derived fields
base = (raw.select(
            "Year", "Month", "DayOfWeek", "Reporting_Airline",
            "Origin", "OriginCityName", "OriginState", "Dest",
            "DepTimeBlk", "DepDelay", "DepDel15", "ArrDelay",
            "Cancelled", "Diverted",
            "CarrierDelay", "WeatherDelay", "NASDelay",
            "SecurityDelay", "LateAircraftDelay")
        .withColumn("is_delayed", F.when(F.col("DepDel15") == 1, 1).otherwise(0))
        .withColumn("is_operated",
                    F.when((F.col("Cancelled") == 0) & (F.col("Diverted") == 0), 1).otherwise(0))
        .withColumn("dep_hour", F.substring("DepTimeBlk", 1, 2).cast("int"))
        .withColumn("route", F.concat_ws("-", "Origin", "Dest"))
        .withColumn("season",
                    F.when(F.col("Month").isin(12, 1, 2), "Winter")
                     .when(F.col("Month").isin(3, 4, 5), "Spring")
                     .when(F.col("Month").isin(6, 7, 8), "Summer")
                     .otherwise("Fall")))

base = base.persist(StorageLevel.MEMORY_AND_DISK)


# 3) Aggregation helper (same metrics for every table)
def agg(df, keys):
    out = (df.groupBy(*keys).agg(
        F.count("*").alias("total_flights"),
        F.sum("Cancelled").alias("cancelled"),
        F.sum("is_delayed").alias("delayed"),
        F.sum("is_operated").alias("operated"),
        F.round(F.avg("DepDelay"), 2).alias("avg_dep_delay"),
        F.round(F.avg("ArrDelay"), 2).alias("avg_arr_delay")))
    return (out
            .withColumn("delay_rate", F.round(100 * F.col("delayed") / F.col("operated"), 2))
            .withColumn("cancellation_rate", F.round(100 * F.col("cancelled") / F.col("total_flights"), 2))
            .drop("operated"))


def clear_table(table):
    # Empty the table first, so re-running the job never duplicates rows
    jvm = spark._sc._gateway.jvm
    conn = jvm.java.sql.DriverManager.getConnection(
        URL, OPTS["user"], OPTS["password"])
    stmt = conn.createStatement()
    stmt.execute("TRUNCATE TABLE " + table)
    stmt.close()
    conn.close()


def save(df, table):
    clear_table(table)
    (df.write.format("jdbc").options(**OPTS)
       .option("url", URL)
       .option("dbtable", table)
       .option("batchsize", 50000)
       .mode("append")
       .save())
    print("saved", table, flush=True)


save(agg(base, ["Reporting_Airline"])
     .withColumnRenamed("Reporting_Airline", "airline"), "agg_airline")

save(agg(base, ["Origin", "OriginCityName", "OriginState"])
     .withColumnRenamed("Origin", "airport")
     .withColumnRenamed("OriginCityName", "city")
     .withColumnRenamed("OriginState", "state"), "agg_airport")

save(agg(base, ["dep_hour"]), "agg_hour")

save(agg(base, ["Year", "Month", "season"])
     .withColumnRenamed("Year", "year")
     .withColumnRenamed("Month", "month"), "agg_month")

save(agg(base, ["DayOfWeek"])
     .withColumnRenamed("DayOfWeek", "day_of_week"), "agg_dayofweek")

save(agg(base, ["route", "Origin", "Dest"])
     .withColumnRenamed("Origin", "origin")
     .withColumnRenamed("Dest", "dest"), "agg_route")

# 4) Delay causes (total minutes per cause)
cause_cols = {
    "Carrier": "CarrierDelay",
    "Weather": "WeatherDelay",
    "NAS": "NASDelay",
    "Security": "SecurityDelay",
    "Late Aircraft": "LateAircraftDelay",
}
sums = base.agg(*[F.sum(c).alias(c) for c in cause_cols.values()]).first()
total = sum((sums[c] or 0.0) for c in cause_cols.values())
rows = [(name, float(sums[c] or 0.0), round(100.0 * float(sums[c] or 0.0) / total, 2))
        for name, c in cause_cols.items()]
causes = spark.createDataFrame(rows, "cause string, total_delay_minutes double, pct double")
save(causes, "agg_delay_cause")

spark.stop()