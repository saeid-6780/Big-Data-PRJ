from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import uuid_from_time
from datetime import datetime,time,date, timedelta
import random
import numpy as np
import os
import json
from kafka import KafkaConsumer

def getDBSession():
    #"""Create and get a Cassandra session"""
    cloud_config= {
            'secure_connect_bundle': os.environ.get('ASTRA_PATH_TO_SECURE_BUNDLE')
    }
    auth_provider = PlainTextAuthProvider(os.environ.get('ASTRA_CLIENT_ID'), os.environ.get('ASTRA_CLIENT_SECRET'))
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    return session

def kafkaconsumer(session):
    #consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
    consumer = KafkaConsumer('taxi-topic',bootstrap_servers=['localhost:9092'],     auto_offset_reset='earliest')
    start_time=datetime.now()
    print("Start time: "+str(start_time))
    insert_query = session.prepare("\
                INSERT INTO bd_prj_kayspace.week_1_taxi_table(lat, lon,uuid,base,timestamp,time,bucket1w)\
                VALUES (?, ?, ?, ?, ?,?,?)\
                IF NOT EXISTS\
                ")
    week=1
    start_dt = date(2014, 9, 1)
    i=0    

    for event in consumer:
         record=json.loads(event.value)
         #generateDataEntrypoint(record,insert_query,session)
         #print(record['Date/Time'])
         #print (record)
         try:
             datetime_record = datetime.strptime(record['Date/Time'], '%m/%d/%Y %H:%M:%S')
             date_record=datetime_record.date()
             time_record=datetime_record.time()
             if(((date_record-start_dt).days)>=7):
                 week=week+1
                 start_dt=start_dt+timedelta(days=7)
             #print(start_dt)
             #print(week)
             #print(date_record+" "+time_record)
             
             session.execute(insert_query, [float(record['Lat']),float(record['Lon']),int(record['uuid']),record['Base'],uuid_from_time(datetime_record),datetime_record,week])
             i=i+1
             print("Add record " + str(i) , end="\r")
             if(i==100000):
                 return start_time
             #print("Success")
         except Exception as e:
             print(e)

    return start_time
    #assert isinstance(msg.value, dict) #print (msg.headers)
    #metrics = consumer.metrics()

def main():
    session = getDBSession()
    start_time=kafkaconsumer(session)
    #generateDataEntrypoint(session)
    
    end_time=datetime.now()
    print("End Time: "+str(end_time))
    print("duration: {}".format(end_time-start_time))

if __name__ == "__main__":
    main()
