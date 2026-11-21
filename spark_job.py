import os

# Set environment variables for Java and Hadoop (adjust paths as needed for your system)
# Set Java home (adjust if needed)
os.environ['JAVA_HOME'] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.10002-hotspot"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['hadoop.home.dir'] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session with required packages
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("DeadWeightMatcher")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.clickhouse:clickhouse-jdbc:0.6.0",
    )
    .config("spark.jars.excludes", "org.apache.hadoop:hadoop-client-api,org.apache.hadoop:hadoop-client-runtime")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.warehouse.dir", "file:///c:/tmp/warehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define schemas for the incoming JSON payloads
truck_schema = StructType([
    StructField("truck_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("status", StringType()),
    StructField("event_time", StringType()),
])

load_schema = StructType([
    StructField("load_id", StringType()),
    StructField("origin_lat", DoubleType()),
    StructField("origin_lon", DoubleType()),
    StructField("destination", StringType()),
    StructField("event_time", StringType()),
])

# Read truck telemetry from Kafka
trucks_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "truck-telemetry")
    .load()
    .select(from_json(col("value").cast("string"), truck_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withWatermark("event_time", "10 minutes")
)

# Read load board data from Kafka
loads_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "load-board-available")
    .load()
    .select(from_json(col("value").cast("string"), load_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withWatermark("event_time", "10 minutes")
)

# Alias DataFrames for join clarity
trucks_alias = trucks_df.alias("t")
loads_alias = loads_df.alias("l")

# Matching logic: empty trucks within ~0.5 degrees (~50km) and within 15 minutes of a load origin
matched_df = (
    trucks_alias.filter(col("t.status") == "Empty")
    .join(
        loads_alias,
        expr(
            "abs(t.lat - l.origin_lat) < 0.5 AND "
            "abs(t.lon - l.origin_lon) < 0.5 AND "
            "t.event_time BETWEEN l.event_time - interval 15 minutes AND l.event_time + interval 15 minutes"
        ),
        "inner",
    )
    .select(
        col("t.truck_id"),
        col("l.load_id"),
        col("l.destination"),
        col("t.event_time").alias("match_time"),
    )
)

# Function to write each micro‑batch to ClickHouse
def write_to_clickhouse(df, epoch_id):
    (
        df.write.format("jdbc")
        .option("url", "jdbc:clickhouse://localhost:8123/default")
        .option("dbtable", "matched_trips")
        .option("user", "default")
        .option("password", "")
        .mode("append")
        .save()
    )

# Start the streaming query
query = (
    matched_df.writeStream
    .foreachBatch(write_to_clickhouse)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark-checkpoints")
    .start()
)

# Keep the query alive until termination (Ctrl‑C to stop)
query.awaitTermination()
