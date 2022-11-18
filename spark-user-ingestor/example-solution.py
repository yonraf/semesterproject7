from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import explode, split, to_json, array, col, struct, udf, from_json
from operator import add
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()


# Create SparkSession and configure it
spark = SparkSession.builder.appName('user_ingestor') \
    .config('spark.master', 'spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max', 1) \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.streaming.checkpointLocation', 'hdfs://namenode:9000/stream-checkpoint/') \
    .config('spark.streaming.concurrentJobs', 2)\
    .getOrCreate()


def usersProcessing():

    # Create a read stream from Kafka and a topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("startingOffsets", "earliest")\
        .option("subscribe", "users") \
        .load()

    # Generate event scheme
    schema = StructType([
        StructField("login", StringType()),
        StructField("location", StringType()),
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
    df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Send data back to kafka
    df.select(to_json(struct([df[x] for x in df.columns])).alias("value")).select("value")\
        .writeStream\
        .format('kafka')\
        .outputMode("append")\
        .option("kafka.bootstrap.servers", "kafka:9092")\
        .option("topic", "processed_users")\
        .start()\
        .awaitTermination()

usersProcessing()

'''
def mapToOriginScheme(dataFrame):
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

    dataFrame = dataFrame.selectExpr("CAST(value AS STRING)")
    dataFrame = dataFrame.select(
        from_json(col("value"), schema).alias("data")).select("data.*")
    return dataFrame


print("Helt Ny Dunya : 00099")
print(df)
df.printSchema()
dataFrame = mapToOriginScheme(df)

dataFrame.printSchema()


dataFrame.select(to_json(struct([dataFrame[x] for x in dataFrame.columns])).alias("value")).select("value")\
    .writeStream\
    .format('kafka')\
    .outputMode("append")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("topic", "processed_events")\
    .start()\
    .awaitTermination()\
    .stop()

spark.stop()
'''