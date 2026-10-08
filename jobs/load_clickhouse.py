from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, lit

spark = SparkSession.builder.appName("load_clickhouse").getOrCreate()

# 1) القراءة من HDFS
df = spark.read.option("header", True).csv("hdfs://namenode:9000/flight/raw/*/*.csv")

def to_int(name):
    return col(name).cast("double").cast("int")

def to_float(name):
    return col(name).cast("double")

# 2) التنضيف: اختيار الأعمدة المهمة وتحويل الأنواع
clean = df.select(
    to_int("Year").alias("Year"),
    to_int("Quarter").alias("Quarter"),
    to_int("Month").alias("Month"),
    to_int("DayOfMonth").alias("DayOfMonth"),
    to_int("DayOfWeek").alias("DayOfWeek"),
    to_date(col("FlightDate")).alias("FlightDate"),
    col("Reporting_Airline"),
    col("Origin"),
    col("OriginCityName"),
    col("OriginState"),
    col("Dest"),
    col("DestCityName"),
    col("DestState"),
    col("DepTimeBlk"),
    to_float("DepDelay").alias("DepDelay"),
    to_float("DepDel15").alias("DepDel15"),
    to_float("ArrDelay").alias("ArrDelay"),
    to_float("ArrDel15").alias("ArrDel15"),
    coalesce(to_int("Cancelled"), lit(0)).alias("Cancelled"),
    coalesce(col("CancellationCode"), lit("")).alias("CancellationCode"),
    coalesce(to_int("Diverted"), lit(0)).alias("Diverted"),
    to_float("Distance").alias("Distance"),
    to_float("CarrierDelay").alias("CarrierDelay"),
    to_float("WeatherDelay").alias("WeatherDelay"),
    to_float("NASDelay").alias("NASDelay"),
    to_float("SecurityDelay").alias("SecurityDelay"),
    to_float("LateAircraftDelay").alias("LateAircraftDelay"),
).filter(col("FlightDate").isNotNull())

# 3) الكتابة في ClickHouse
(clean.repartition(2).write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/flights")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "flights")
    .option("user", "admin")
    .option("password", "admin123")
    .option("batchsize", 50000)
    .mode("append")
    .save())

spark.stop()