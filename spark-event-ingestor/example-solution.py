from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import explode, split, to_json, array, col, struct, udf, from_json
from operator import add
import locale


locale.getdefaultlocale()
locale.getpreferredencoding()


# Create SparkSession and configure it
spark = SparkSession.builder.appName('event_ingestor') \
    .config('spark.master', 'spark://10.123.252.233:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max', 1) \
    .config('spark.sql.streaming.checkpointLocation', 'hdfs://10.123.252.233:9000/stream-checkpoint/') \
    .getOrCreate()


def eventsProcessing():
    # Create a read stream from Kafka and a topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.123.252.233:9092,10.123.252.200:9092,10.123.252.209:9092") \
        .option("startingOffsets", "earliest")\
        .option("subscribe", "events") \
        .load()

    # Generate event scheme
    schema = StructType([
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("actor", StructType([
            StructField("login", StringType())
        ])),
        StructField("repo", StructType([
            StructField("name", StringType())
        ])),
        StructField("payload", StructType([
            StructField("size", StructType([
                StructField("$numberInt", StringType())
            ]))
        ])),
        StructField("created_at", StringType())
    ])

    # Insert data into scheme
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_json(col("value"), schema).alias(
        "data")).select("data.*")

    # Drop unwanted columns (THIS WORKS)
    df = df.withColumn("ID", col("id")) \
        .withColumn("Type", col("type")) \
        .withColumn("User", col("actor.login")) \
        .withColumn("Repo", col("repo.name")) \
        .withColumn("Size", col("payload.size.$numberInt")) \
        .drop(col("actor")) \
        .drop(col("payload"))


    # Write the data to HDFS
    df.writeStream\
        .format('json')\
        .outputMode("append")\
        .option("path", "hdfs://10.123.252.233:9000/events/")\
        .option("checkpointLocation", "hdfs://10.123.252.233:9000/events-checkpoint/")\
        .start()\
        .awaitTermination()


# Launch processing
eventsProcessing()
