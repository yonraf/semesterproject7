from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import explode, split, to_json, array, col, struct, udf, from_json
from operator import add
import locale

locale.getdefaultlocale()
locale.getpreferredencoding()


# Create SparkSession and configure it
spark = SparkSession.builder.appName('repo_ingestor') \
    .config('spark.master', 'spark://10.123.252.233:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max', 1) \
    .config('spark.sql.streaming.checkpointLocation', 'hdfs://10.123.252.233:9000/stream-checkpoint/') \
    .getOrCreate()

# .config('spark.executor.memory', '1g') \


def reposProcessing():
    # Create a read stream from Kafka and a topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.123.252.200:9092") \
        .option("startingOffsets", "earliest")\
        .option("subscribe", "repos") \
        .load()

    # Generate event scheme
    schema = StructType([
        StructField("full_name", StringType()),
        StructField("size", StructType([
            StructField("$numberInt", StringType())
        ])),
        StructField("stargazers_count", StructType([
            StructField("$numberInt", StringType())
        ])),
        StructField("watchers_count", StructType([
            StructField("$numberInt", StringType())
        ])),
        StructField("forks_count", StructType([
            StructField("$numberInt", StringType())
        ])),
        StructField("has_wiki", BooleanType())])

    # Insert data into scheme
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_json(col("value"), schema).alias(
        "data")).select("data.*")

    # Drop unwanted columns (THIS WORKS)
    df = df.withColumn("Name", col("full_name")) \
        .withColumn("Size", col("size.$numberInt")) \
        .withColumn("Stars", col("stargazers_count.$numberInt")) \
        .withColumn("Watchers", col("watchers_count.$numberInt")) \
        .withColumn("Forks", col("forks_count.$numberInt")) \
        .drop(col("stargazers_count")) \
        .drop(col("forks_count")) \
        .drop(col("watchers_count")) \
        .drop(col("full_name"))


    # Write the data to HDFS
    df.writeStream\
        .format('json')\
        .outputMode("append")\
        .option("path", "hdfs://10.123.252.233:9000/repos/")\
        .option("checkpointLocation", "hdfs://10.123.252.233:9000/repos-checkpoint/")\
        .start()\
        .awaitTermination()


# Launch processing
reposProcessing()
