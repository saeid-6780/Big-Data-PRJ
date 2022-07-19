from kafka import KafkaConsumer, KafkaProducer
import json
from json import loads
from csv import DictReader
import datetime
import time

bootstrap_servers = ['localhost:9092']
topic_name = 'taxi-topic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

counter = 1
with open('data-low.csv' , 'r') as new_obj:
    csv_dict_reader = DictReader(new_obj)
    for row in csv_dict_reader:
        row['uuid'] = counter
        counter += 1
        dt = datetime.datetime.strptime(row['Date/Time'], '%m/%d/%Y %H:%M:%S')
        dt = int(datetime.datetime.timestamp(dt))
        time.sleep(0.2)
        ack = producer.send(topic_name, json.dumps(row).encode('utf-8'),timestamp_ms=dt)
        metadata = ack.get()
        print("Add row number " + str(row['uuid']) , end="\r")
        #print(metadata.topic, metadata.partition)
print('finished')
