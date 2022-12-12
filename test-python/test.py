from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import time

# Create an insecure client that works when HDFS has security turned off
client = InsecureClient('http://namenode:9870', user='root')
consumer = KafkaConsumer(bootstrap_servers=['kafka:9092'])

print("TOPICZ 1 : ")
print(consumer.topics())

consumer.subscribe(['processed_events', 'processed_users', 'processed_repos', 'doesntexits'])

print("TOPICZ 2 : ")
print(consumer.topics())

print("azino : ")
print(consumer.assignment())



def upload_to_hdfs(topic, data):

    if topic == "processed_repos":
        if client.status('/repos.json', strict=False) == None:
            with client.write('/repos.json', encoding='utf-8', overwrite=True) as writer:
                writer.write(json.dumps(data))
                writer.write("\n")
                print()
        else:
            with client.write('/repos.json', encoding='utf-8', append=True) as writer:
                writer.write(json.dumps(data))
                writer.write("\n")

    elif topic == "processed_users":
        if client.status('/users.json', strict=False) == None:
            with client.write('/users.json', encoding='utf-8', overwrite=True) as writer:
                writer.write(json.dumps(data))
                writer.write("\n")
        else:
            with client.write('/users.json', encoding='utf-8', append=True) as writer:
                writer.write(json.dumps(data))
                writer.write("\n")

    elif topic == "processed_events":
        if client.status('/events.json', strict=False) == None:
            with client.write('/events.json', encoding='utf-8', overwrite=True) as writer:
                writer.write(json.dumps(data))
                writer.write("\n")
        else:
            with client.write('/events.json', encoding='utf-8', append=True) as writer:
                writer.write(json.dumps(data))
                writer.write("\n")
'''
while True:
    records = consumer.poll()

    for x in records.items():
        for index, element in enumerate(x):
            if index % 2 == 1:
                for data in element:
                    data_string = data.value.decode('utf-8')
                    topic = data.topic
                    upload_to_hdfs(topic, data_string)
'''
# def upload_to_hdfs(topic, data):
#     if topic == "processed_repos":
#         with client.write('/repos.json', encoding='utf-8', append=True) as writer:
#             writer.write(json.dumps(data))
#             writer.write("\n")
#     elif topic == "processed_users":
#         with client.write('/users.json', encoding='utf-8', append=True) as writer:
#             writer.write(json.dumps(data))
#             writer.write("\n")
#     elif topic == "processed_events":
#         with client.write('/events.json', encoding='utf-8', append=True) as writer:
#             writer.write(json.dumps(data))
#             writer.write("\n")


# while True:
#     records = consumer.poll()

#     for x in records.items():
#         for index, element in enumerate(x):
#             if index % 2 == 1:
#                 for data in element:
#                     data_string = data.value.decode('utf-8')
#                     topic = data.topic
#                     upload_to_hdfs(topic, data_string)


# client = InsecureClient('http://namenode:9870', user='root')
# client.write('/repos.json', encoding='utf-8', overwrite=True, data='')
# client.write('/users.json', encoding='utf-8', overwrite=True, data='')
# client.write('/events.json', encoding='utf-8', overwrite=True, data='')