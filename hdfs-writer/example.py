from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import time

# Create an insecure client that works when HDFS has security turned off
client = InsecureClient('http://namenode:9870', user='root')
consumer = KafkaConsumer(bootstrap_servers=['kafka:9092'])

consumer.subscribe(['processed_events', 'processed_users', 'processed_repos'])

def upload_to_hdfs(topic, data):

    if topic == "processed_repos":
        if client.status('/repos.json', strict=False) == None:
            with client.write('/repos.json', encoding='utf-8', overwrite=True) as writer:
                writer.write(data)
                writer.write("\n")
        else:
            with client.write('/repos.json', encoding='utf-8', append=True) as writer:
                writer.write(data)
                writer.write("\n")

    elif topic == "processed_users":
        if client.status('/users.json', strict=False) == None:
            with client.write('/users.json', encoding='utf-8', overwrite=True) as writer:
                writer.write(data)
                writer.write("\n")
        else:
            with client.write('/users.json', encoding='utf-8', append=True) as writer:
                writer.write(data)
                writer.write("\n")

    elif topic == "processed_events":
        if client.status('/events.json', strict=False) == None:
            with client.write('/events.json', encoding='utf-8', overwrite=True) as writer:
                writer.write(data)
                writer.write("\n")
        else:
            with client.write('/events.json', encoding='utf-8', append=True) as writer:
                writer.write(data)
                writer.write("\n")

while True:
    while "processed_repos" not in consumer.topics() or "processed_users" not in consumer.topics() or "processed_events" not in consumer.topics():
        time.sleep(0.001)

    records = consumer.poll()

    for x in records.items():
        for index, element in enumerate(x):
            if index % 2 == 1:
                for data in element:
                    data_string = data.value.decode('utf-8')
                    topic = data.topic
                    upload_to_hdfs(topic, data_string)


