
from urllib.request import build_opener
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from flask import Flask
import json



import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

app = Flask(__name__)

eventsPath = "hdfs://namenode:9000/events/events.json"
reposPath =  "hdfs://namenode:9000/repos/repos.json"
usersPath =  "hdfs://namenode:9000/users/users.json"

# Limit cores to 1, and tell each executor to use one core = only one executor is used by Spark
# conf = SparkConf().set('spark.executor.cores', 1).set('spark.cores.max',1).set('spark.executor.memory', '1g').set('spark.driver.host', '127.0.0.1')
# sc = SparkContext(master='local', appName='pyspark-local', conf=conf)
spark = SparkSession.builder.appName('pyspark').config("yarn.nodemanager.vmem-check-enabled","false").getOrCreate()

@app.route('/')
def index():
    return 'Ya kalb'


@app.route('/user/most/repos')
def most_repos(): 
   
    df = spark.read.json(usersPath)
    # result = df.groupBy('Username').max('Repositories').collect()
    
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, INT(Repositories) FROM users WHERE Repositories in (select max(INT(Repositories)) FROM users)")

    print(query)

    row = query.collect()
    print(row)
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


@app.route('/user/least/repos')
def least_repos():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, INT(Repositories) FROM users WHERE Repositories in (select min(INT(Repositories)) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

@app.route('/user/most/followers')
def most_followers():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, Followers FROM users WHERE Followers in (select max(INT(Followers) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


@app.route('/user/least/followers')
def least_followers():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, Followers FROM users WHERE Followers in (select min(INT(Followers)) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


@app.route('/user/most/following')
def most_followings():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, Following FROM users WHERE Following in (select max(INT(Following)) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


@app.route('/user/least/following')
def least_followings():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, Following FROM users WHERE Following in (select min(INT(Following)) FROM users)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

# What repository has the (most/least) number of (stars/forks/watchers)?
app.route('/repos/most/stars')
def most_stars():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Stars FROM repos WHERE Stars in (select max(INT(Stars)) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


app.route('/repos/least/stars')
def least_stars():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Stars FROM repos WHERE Stars in (select min(INT(Stars)) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/repos/most/watchers')
def most_watchers():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Watchers FROM repos WHERE Watchers in (select max(INT(Watchers)) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/repos/least/watchers')
def least_watchers():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Watchers FROM repos WHERE Watchers in (select min(INT(Watchers)) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/repos/most/forks')
def most_forks():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Forks FROM repos WHERE Forks in (select max(INT(Forks)) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)

app.route('/repos/least/forks')
def least_forks():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Forks FROM repos WHERE Forks in (select min(INT(Forks)) FROM repos)")

    row = query.collect()
    data = row[0].asDict()

    print(data)
    
    return json.dumps(data)


# How many repositories (has/don't have) a wiki linked to it?
@app.route('/repos/no/wiki')
def has_no_wiki():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

      
    query = spark.sql("SELECT COUNT(*) FROM repos WHERE has_wiki = '0' ")

    # row = df.filter(df["has_wiki"] == False).collect()
    
    print(query)
    print(query.collect())

    # data = {
    #     'amount': row.length
    # }
    
    return json.dumps(query.collect)


@app.route('/repos/has/wiki')
def has_wiki():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

      
    query = spark.sql("SELECT COUNT(*) FROM repos WHERE has_wiki = '1' ")

    # row = df.filter(df["has_wiki"] == True).collect()
    
    

    # data = {
    #     'amount': row.length
    # }


    # print(data)
    
    return json.dumps(query.collect)






if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3000)