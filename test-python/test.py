from urllib.request import build_opener
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import json
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

spark = SparkSession.builder.appName('pyspark').getOrCreate()

eventsPath = "hdfs://namenode:9000/events/*.json"

df = spark.read.json(eventsPath)
df.createOrReplaceTempView('events')

query = spark.sql('SELECT Type, COUNT(Type) AS count FROM events GROUP BY Type ORDER BY COUNT(Type) LIMIT 1')


data = query.collect()[0][0]

print('The least common event is '+ data) 