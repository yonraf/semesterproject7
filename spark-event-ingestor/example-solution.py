from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import explode, split, to_json, array, col, struct, udf, from_json
from operator import add
import locale


locale.getdefaultlocale()
locale.getpreferredencoding()


# Create SparkSession and configure it
spark = SparkSession.builder.appName('event_ingestor') \
    .config('spark.master', 'spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max', 1) \
    .config('spark.sql.streaming.checkpointLocation', 'hdfs://namenode:9000/stream-checkpoint/') \
    .getOrCreate()

# .config('spark.executor.memory', '1g') \


def eventsProcessing():
    # Create a read stream from Kafka and a topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
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

    client = InsecureClient('http://namenode:9870', user='root')

    # Send data back to kafka
    df.select(to_json(struct([df[x] for x in df.columns])).alias("value")).select("value")\
        .writeStream\
        .format('kafka')\
        .outputMode("append")\
        .option("kafka.bootstrap.servers", "kafka:9092")\
        .option("topic", "processed_events")\
        .start()\
        .awaitTermination()

# DELETE THIS IN FUTURE IF NOT USED
def uploadProccessed():
    schema = StructType([
        StructField("ID", StringType()),
        StructField("Type", StringType()),
        StructField("Repo", StringType()),
        StructField("created_at", StringType()),
        StructField("User", StringType()),
        StructField("Size", StringType())
    ])

    # Create a read stream from Kafka and a topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("startingOffsets", "earliest")\
        .option("subscribe", "processed_events") \
        .load()

    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_json(col("value"), schema).alias(
        "data")).select("data.*")

    df\
        .writeStream\
        .format('json')\
        .option("path", "hdfs://namenode:9000/data/events.json") \
        .outputMode("append") \
        .start().awaitTermination()


# Launch processing
eventsProcessing()
