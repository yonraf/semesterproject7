
from urllib.request import build_opener
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from flask import Flask
import json



import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

app = Flask(__name__)

servers = "hdfs://namenode:9000/userTest2.json"

# Limit cores to 1, and tell each executor to use one core = only one executor is used by Spark
# conf = SparkConf().set('spark.executor.cores', 1).set('spark.cores.max',1).set('spark.executor.memory', '1g').set('spark.driver.host', '127.0.0.1')
# sc = SparkContext(master='local', appName='pyspark-local', conf=conf)
spark = SparkSession.builder.appName('pyspark').getOrCreate()

@app.route('/')
def root(): 
   
    df = spark.read.json(servers)
    df.createOrReplaceTempView("users")

    most_repos = spark.sql("SELECT username, repos FROM users WHERE repos in (select max(repos) FROM users)")

    print(most_repos.toJSON())
    
    return json.load(most_repos.toJSON())


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3000)