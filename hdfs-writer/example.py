from kafka import KafkaConsumer
from hdfs import InsecureClient
import json

# Create an insecure client that works when HDFS has security turned off
consumer = KafkaConsumer('processed_events', bootstrap_servers=['kafka:9092'])
client = InsecureClient('http://namenode:9870', user='root')

for data in consumer:
    data = json.loads(data.value)        
    print(data)

    with client.write('/events.txt', encoding='utf-8', overwrite=True) as writer:
        writer.write(json.dumps(data))
        
