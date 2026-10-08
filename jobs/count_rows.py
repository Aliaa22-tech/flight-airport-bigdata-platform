from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("count_rows").getOrCreate()

df = spark.read.option("header", True).csv("hdfs://namenode:9000/flight/raw/*/*.csv")

print("NUMBER OF COLUMNS:", len(df.columns))
print("NUMBER OF ROWS:", df.count())

df.select("FlightDate", "Reporting_Airline", "Origin", "Dest", "DepDelay", "Cancelled").show(5)

spark.stop()