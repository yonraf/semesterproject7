
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
def index():
    return 'Hello World'

@app.route('/mostrepos')
def most_repos(): 
   
    df = spark.read.json(servers)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT username, repos FROM users WHERE repos in (select max(repos) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


@app.route('/leastrepos')
def least_repos():
    df = spark.read.json(servers)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT username, repos FROM users WHERE repos in (select min(repos) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

@app.route('/mostfollowers')
def most_followers():
    df = spark.read.json(servers)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT username, followers FROM users WHERE followers in (select max(followers) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


@app.route('/leastfollowers')
def least_followers():
    df = spark.read.json(servers)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT username, followers FROM users WHERE followers in (select min(followers) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


@app.route('/mostfollowings')
def most_followings():
    df = spark.read.json(servers)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT username, following FROM users WHERE following in (select max(following) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


@app.route('/leastfollowings')
def least_followings():
    df = spark.read.json(servers)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT username, following FROM users WHERE following in (select min(following) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


app.route('/moststars')
def most_stars():
    
    df = spark.read.json(servers)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Stars FROM repos WHERE Stars in (select max(Stars) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


app.route('/leaststars')
def least_stars():
    
    df = spark.read.json(servers)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Stars FROM repos WHERE Stars in (select min(Stars) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/mostwatchers')
def most_watchers():
    
    df = spark.read.json(servers)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Watchers FROM repos WHERE Watchers in (select max(Watchers) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/leastwatchers')
def least_watchers():
    
    df = spark.read.json(servers)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Watchers FROM repos WHERE Watchers in (select min(Watchers) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/mostforks')
def most_forks():
    
    df = spark.read.json(servers)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Forks FROM repos WHERE Forks in (select max(Forks) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/leastforks')
def least_forks():
    
    df = spark.read.json(servers)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Forks FROM repos WHERE Forks in (select min(Forks) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/hasnowiki')
def has_no_wiki():
    
    df = spark.read.json(servers)
    # df.createOrReplaceTempView("repos")

      
    #  query = spark.sql("SELECT Name, Forks FROM repos WHERE has_wiki = false")

    row = df.filter(df["has_wiki"] == False).collect()
    
    

    data = {
        'amount': row.length
    }


    print(data)
    
    return json.dumps(data)


@app.route('/haswiki')
def has_no_wiki():
    
    df = spark.read.json(servers)
    # df.createOrReplaceTempView("repos")

      
    #  query = spark.sql("SELECT Name, Forks FROM repos WHERE has_wiki = false")

    row = df.filter(df["has_wiki"] == True).collect()
    
    

    data = {
        'amount': row.length
    }


    print(data)
    
    return json.dumps(data)






if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3000)