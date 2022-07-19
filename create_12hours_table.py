from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import uuid_from_time
from datetime import datetime,time, timedelta
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

def in_between(now,start,end):
    if start <= end:
        return start <= now < end
    else: 
        return start <= now or now < end

def kafkaconsumer(session):
    consumer = KafkaConsumer('taxi-topic',bootstrap_servers=['localhost:9092'],     auto_offset_reset='earliest')
    start_time=datetime.now()
    print("Start time: "+str(start_time))
    insert_query = session.prepare("\
                INSERT INTO bd_prj_kayspace.houres_12_taxi_table (lat, lon,uuid,base,timestamp,bucket12h,bucket6h,day)\
                VALUES (?, ?, ?, ?, ?,?,?,?)\
                IF NOT EXISTS\
                ")
    i=0
    for event in consumer:
         record=json.loads(event.value)
         try:
             datetime_record = datetime.strptime(record['Date/Time'], '%m/%d/%Y %H:%M:%S')
             date_record=datetime_record.date()
             time_record=datetime_record.time()
             if(in_between(time_record,time(0),time(12))):
                 bucket12h=1
                 bucket6h=2
                 if(in_between(time_record,time(0),time(6))):
                     bucket6h=1
             else:
                 bucket12h=2
                 bucket6h=2
                 if(in_between(time_record,time(12),time(18))):
                     bucket6h=1
             session.execute(insert_query, [float(record['Lat']),float(record['Lon']),int(record['uuid']),record['Base'],uuid_from_time(datetime_record),bucket12h,bucket6h,date_record.strftime('%m/%d/%Y')])
             i=i+1
             print("Add record " + str(i) , end="\r")
             if(i==100000):
                 return start_time
         except Exception as e:
             print(e)
    return start_time
def main():
    session = getDBSession()
    start_time=kafkaconsumer(session)
    end_time=datetime.now()
    print("End Time: "+str(end_time))
    print("duration: {}".format(end_time-start_time))
if __name__ == "__main__":
    main()
