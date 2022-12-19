

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import json



import locale
locale.getdefaultlocale()
locale.getpreferredencoding()




# Limit cores to 1, and tell each executor to use one core = only one executor is used by Spark
# conf = SparkConf().set('spark.executor.cores', 1).set('spark.cores.max',1).set('spark.executor.memory', '1g').set('spark.driver.host', '127.0.0.1')
# sc = SparkContext(master='local', appName='pyspark-local', conf=conf)
spark = SparkSession.builder.appName('pyspark').getOrCreate()






spark = SparkSession.builder.appName('pyspark').getOrCreate()

eventsPath = "tilføj path"

df = spark.read.json(eventsPath)
df.createOrReplaceTempView('events')

query = spark.sql('SELECT Type, COUNT(Type) AS count FROM events GROUP BY Type ORDER BY COUNT(Type) DESC LIMIT 1')


data = query.collect()[0][0]

print('The most common event is '+ data) 