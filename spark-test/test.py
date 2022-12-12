
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

    query = spark.sql("SELECT Username, INT(Repositories) FROM users WHERE Repositories in (select max(INT(Repositories)) FROM users) LIMIT 1")

    data = query.collect()[0]
    
    username = data['Username']
    repos = data['Repositories']
    return username + ' is the user with most repositories of ' + str(repos)


@app.route('/user/least/repos')
def least_repos():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, INT(Repositories) FROM users WHERE Repositories in (select min(INT(Repositories)) FROM users) LIMIT 1")

    data = query.collect()[0]
    
    username = data['Username']
    repos = data['Repositories']
    return username + ' is the user with least repositories of ' + str(repos)

@app.route('/user/most/followers')
def most_followers():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, Followers FROM users WHERE Followers in (select max(INT(Followers) FROM users)")

    data = query.collect()[0]
    
    username = data['Username']
    followers = data['Followers']

    return username + ' is the user with most follower of ' + str(followers)


@app.route('/user/least/followers')
def least_followers():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, Followers FROM users WHERE Followers in (select min(INT(Followers)) FROM users)")

    data = query.collect()[0]
    
    username = data['Username']
    followers = data['Followers']
    
    return username + ' is the user with least follower of ' + str(followers)


@app.route('/user/most/following')
def most_followings():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, Following FROM users WHERE Following in (select max(INT(Following)) FROM users)")

    data = query.collect()[0]
    
    username = data['Username']
    following = data['Following']
    
    return username + ' is the user who are following most users with ' + str(following)


@app.route('/user/least/following')
def least_followings():
    df = spark.read.json(usersPath)
    df.createOrReplaceTempView("users")

    query = spark.sql("SELECT Username, Following FROM users WHERE Following in (select min(INT(Following)) FROM users)")

    data = query.collect()[0]
    
    username = data['Username']
    following = data['Following']
    
    return username + ' is the user with least follower of ' + str(following)

# What repository has the (most/least) number of (stars/forks/watchers)?
@app.route('/repos/most/stars')
def most_stars():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Stars FROM repos WHERE Stars in (select max(INT(Stars)) FROM repos)")

    data = query.collect()[0]
    
    name = data['Name']
    stars = data['Stars']
    
    return name + ' is the repository with most stars of ' + str(stars)


@app.route('/repos/least/stars')
def least_stars():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Stars FROM repos WHERE Stars in (select min(INT(Stars)) FROM repos)")

    data = query.collect()[0]
    
    name = data['Name']
    stars = data['Stars']
    
    return name + ' is the repository with least stars of '+ str(stars)

@app.route('/repos/most/watchers')
def most_watchers():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Watchers FROM repos WHERE Watchers in (select max(INT(Watchers)) FROM repos)")

    data = query.collect()[0]
    
    name = data['Name']
    watchers = data['Watchers']
    
    return name + ' is the repository with most watchers of ' + str(watchers)

@app.route('/repos/least/watchers')
def least_watchers():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Watchers FROM repos WHERE Watchers in (select min(INT(Watchers)) FROM repos)")

    data = query.collect()[0]
    
    name = data['Name']
    watchers = data['Watchers']
    
    return name + ' is the repository with least watchers of ' + str(watchers)

@app.route('/repos/most/forks')
def most_forks():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Forks FROM repos WHERE Forks in (select max(INT(Forks)) FROM repos)")

    data = query.collect()[0]
    
    name = data['Name']
    forks = data['Forks']
    
    return name + ' is the repository with most forks of ' + str(forks)

@app.route('/repos/least/forks')
def least_forks():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

    query = spark.sql("SELECT Name, Forks FROM repos WHERE Forks in (select min(INT(Forks)) FROM repos)")

    data = query.collect()[0]
    
    name = data['Name']
    forks = data['Forks']
    
    return name + ' is the repository with least forks of ' + str(forks)


# How many repositories (has/don't have) a wiki linked to it?
# @app.route('/repos/no/wiki')
# def has_no_wiki():
    
#     df = spark.read.json(reposPath)
#     df.createOrReplaceTempView("repos")

      
#     query = spark.sql("SELECT COUNT(*) FROM repos WHERE has_wiki = '0' ")

#     # row = df.filter(df["has_wiki"] == False).collect()
    
#     print(query)

#     # data = {
#     #     'amount': row.length
#     # }
    
#     return query.collect()


@app.route('/repos/has/wiki')
def has_wiki():
    
    df = spark.read.json(reposPath)
    df.createOrReplaceTempView("repos")

      
    query = spark.sql("SELECT COUNT(*) as wiki FROM repos WHERE has_wiki = '1' ")

    # row = df.filter(df["has_wiki"] == True).collect()
    
    data = query.collect()[0][0]

    print(query)

    # data = {
    #     'amount': row.length
    # }
    
    return 'A total of '+ str(data) + ' repositories has a wiki'

# What is the most/least common event?
@app.route('/event/most/common')
def most_common_event():

    df = spark.read.json(eventsPath)
    df.createOrReplaceTempView('events')

    query = spark.sql(
        'SELECT Type, COUNT(Type) AS count FROM events GROUP BY Type ORDER BY COUNT(Type) DESC LIMIT 1')


    data = query.collect()[0][0]

    return 'The most common event is '+ data

@app.route('/event/least/common')
def least_common_event():

    df = spark.read.json(eventsPath)
    df.createOrReplaceTempView('events')

    query = spark.sql(
        'SELECT Type, COUNT(Type) AS count FROM events GROUP BY Type ORDER BY COUNT(Type) LIMIT 1')

    data = query.collect()[0][0]

    return 'The least common event is '+ data

@app.route('/event/first')
def first_event():

    df = spark.read.json(eventsPath)
    df.createOrReplaceTempView('events')

    query = spark.sql(
        'SELECT TYPE FROM events LIMIT 1'
    )

    data = query.collect()[0][0]
    

    return 'The first event is '+ data

@app.route('/event/last')
def last_event():

    df = spark.read.json(eventsPath)
    df.createOrReplaceTempView('events')

    query = spark.sql(
        'SELECT LAST (TYPE) as Event FROM events'
    )

    data = query.collect()[0][0]

    return 'The last event is '+ data


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3000)