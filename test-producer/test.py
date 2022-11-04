from kafka import KafkaProducer
from hdfs import InsecureClient
from collections import Counter
from json import dumps, loads
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, to_json, array, col, struct, udf
from operator import add
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()




"""  ---  WRITE FROM LOCAL TO KAFKA TOPICS  ---  """
# users get inserted into kafka
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
with open('./users.json') as f:
    counter = 0
    lines = f.readlines()
    for line in lines:
        b = bytes(line, 'utf-8')
        producer.send('users',value=b) 
        counter += 1
        if counter > 3:
            break


# events get inserted into kafka
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
with open('./events.json') as f:
    counter = 0
    lines = f.readlines()
    for line in lines:
        b = bytes(line, 'utf-8')
        producer.send('events',value=b)
        counter += 1
        if counter > 3:
            break

# repos get inserted into kafka
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
with open('./repos.json') as f:
    counter = 0
    lines = f.readlines()
    for line in lines:
        b = bytes(line, 'utf-8')
        producer.send('repos',value=b)
        counter += 1
        if counter > 3:
            break
    

"""  ---  WRITE FROM KAFKA TO SPARK  --- 
# Create SparkSession and configure it
spark = SparkSession.builder.appName('streamTest') \
    .config('spark.master','spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max',1) \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.streaming.checkpointLocation','hdfs://namenode:9000/stream-checkpoint/') \
    .getOrCreate()

# Create a read stream from Kafka and a topic
user_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("startingOffsets", "earliest")\
    .option("subscribe", "users") \
    .load()

print(user_df.selectExpr("CAST(value AS STRING)"))

# Cast to string
sentences = user_df.selectExpr("CAST(value AS STRING)")
# Call our user defined function to do the work (this is by far the easiest way to work with Spark as it avoids the need to work within DataFrames)
result = sentences.withColumn("sentiment", sentimentAnalysis(sentences.value))

# Create a Kafka write stream, with the output mode "complete"
result.select(to_json(struct([result[x] for x in result.columns])).alias("value")).select("value")\
    .writeStream\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "Filtered-users") \
    .outputMode("append") \
    .start().awaitTermination()

  
#client = InsecureClient('http://namenode:9870', user='root')



with client.read('/users.json', encoding='utf-8',) as reader:
    for line in reader:
        producer.send('users',value=line) 
        sleep(3)
"""






    

