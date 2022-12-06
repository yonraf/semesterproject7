from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import explode, split, to_json, array, col, struct, udf, from_json
from operator import add
import locale

locale.getdefaultlocale()
locale.getpreferredencoding()


# Create SparkSession and configure it
spark = SparkSession.builder.appName('repo_ingestor') \
    .config('spark.master', 'spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max', 1) \
    .config('spark.sql.streaming.checkpointLocation', 'hdfs://namenode:9000/stream-checkpoint/') \
    .getOrCreate()

    #.config('spark.executor.memory', '1g') \



def reposProcessing():
    # Create a read stream from Kafka and a topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
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
    df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Drop unwanted columns (THIS WORKS)
    df = df.withColumn("Name",col("full_name")) \
    .withColumn("Size",col("size.$numberInt")) \
    .withColumn("Stars",col("stargazers_count.$numberInt")) \
    .withColumn("Stars",col("watchers_count.$numberInt")) \
    .withColumn("Forks",col("forks_count.$numberInt"))

    # Send data back to kafka
    df.select(to_json(struct([df[x] for x in df.columns])).alias("value")).select("value")\
        .writeStream\
        .format('kafka')\
        .outputMode("append")\
        .option("kafka.bootstrap.servers", "kafka:9092")\
        .option("topic", "processed_repos")\
        .start()\
        .awaitTermination()

# Launch processing
reposProcessing()

