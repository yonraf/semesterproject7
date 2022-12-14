import time
from kafka import KafkaProducer, KafkaConsumer
from hdfs import InsecureClient
from collections import Counter
from json import dumps, loads
from operator import add
import requests

""" ---  WRITE FROM LOCAL TO KAFKA TOPICS  --- """
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=["10.123.252.233:9092","10.123.252.200:9092","10.123.252.209:9092"])
        time.sleep(0.5)
        break
    except:
        time.sleep(0.5)
    
time.sleep(1)

with open('./batch_users.json') as f:
    counter = 0
    lines = f.readlines()
    for line in lines:
        b = bytes(line, 'utf-8')
        producer.send('users',value=b)
        counter += 1

time.sleep(0.5)

# events get inserted into kafka
with open('./batch_events.json') as f:
    counter = 0
    lines = f.readlines()
    for line in lines:
        b = bytes(line, 'utf-8')
        producer.send('events',value=b)
        counter += 1


time.sleep(0.5)


# repos get inserted into kafka
with open('./batch_repos.json') as f:
    counter = 0
    lines = f.readlines()
    for line in lines:
        b = bytes(line, 'utf-8')
        producer.send('repos',value=b)
        counter += 1


time.sleep(1.5) 