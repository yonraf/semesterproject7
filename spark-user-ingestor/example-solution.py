from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import explode, split, to_json, array, col, struct, udf, from_json
from operator import add
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()


# Create SparkSession and configure it
spark = spark = SparkSession.builder.appName('user_ingestor').master('local').getOrCreate()

    #     SparkSession.builder.appName('user_ingestor') \
    # .config('spark.master', 'spark://10.123.252.233:7077') \
    # .config('spark.executor.cores', 1) \
    # .config('spark.cores.max', 1) \
    # .config('spark.sql.streaming.checkpointLocation', 'hdfs://10.123.252.233:9000/stream-checkpoint/') \
    # .getOrCreate()


def usersProcessing():

    # Create a read stream from Kafka and a topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.123.252.233:9092,10.123.252.200:9092,10.123.252.209:9092") \
        .option("startingOffsets", "earliest")\
        .option("subscribe", "users") \
        .load()

    # Generate event scheme
    schema = StructType([
        StructField("login", StringType()),
        #StructField("location", StringType()),
        StructField("public_repos", StructType([
            StructField("$numberInt", StringType())
        ])), 
        StructField("followers", StructType([
            StructField("$numberInt", StringType())
        ])),
        StructField("following", StructType([
            StructField("$numberInt", StringType())
        ])),
        StructField("created_at", StringType())
    ])

    # Insert data into scheme
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_json(col("value"), schema).alias(
        "data")).select("data.*")

    # Drop unwanted columns
    df = df.withColumn("Username", col("login")) \
        .withColumn("Followers", col("followers.$numberInt")) \
        .withColumn("Following", col("following.$numberInt")) \
        .withColumn("Repositories", col("public_repos.$numberInt")) \
        .drop(col("public_repos")) \
        .drop(col("login"))        


    # Write the data to HDFS
    df.writeStream\
        .format('json')\
        .outputMode("append")\
        .option("path", "hdfs://10.123.252.233:9000/users/")\
        .option("checkpointLocation", "hdfs://10.123.252.233:9000/users-checkpoint/")\
        .start()\
        .awaitTermination()
    

usersProcessing()