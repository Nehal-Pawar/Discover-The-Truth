############################################################
# This python script is a producer for kafka.
# To send it to kafka, each json is read from s3 
# and each tweet is send to kafka cluster
#
# parameter
# config.py file to get AWS access key and secret keys
# KafkaPorducer sends each json to kafka cluster
############################################################

import boto3
import io
import json
from multiprocessing import Process
import botocore
import pandas as pd
from kafka import KafkaProducer
import sys
import _thread
sys.path.insert(1, '/home/ubuntu/')
from config import (aws_access_key, aws_secret_key)

Pros = []



def SendtoCluster(json_data,producer):
    for i in range(len(json_data)):
        producer.send('twitter', json_data[i])
        producer.flush()
        #print(json_data[i])

def main():

    # Kafka Producer 
    
    producer = KafkaProducer(bootstrap_servers = 'ip-10-0-0-37.us-west-2.compute.internal:9092,ip-10-0-0-12.us-west-2.compute.internal:9092,ip-10-0-0-20.us-west-2.compute.internal:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    s3 = boto3.resource('s3',aws_access_key_id = aws_access_key , aws_secret_access_key = aws_secret_key)

    bucket_name='twitter2017'
    bucket = s3.Bucket('twitter2017')


    #for each sub folder find all .json files
    for object in bucket.objects.all():
        if object.key.endswith(".json"):
            print(object.key)
            url = "https://twitter2017.s3-us-west-2.amazonaws.com/" + object.key
            print(url)
            data_in_bytes = s3.Object(bucket_name, object.key).get()['Body'].read()

            #Decode it in 'utf-8' format
            decoded_data = data_in_bytes.decode('utf-8')

            #I used io module for creating a StringIO object.
            stringio_data = io.StringIO(decoded_data)

            #Now just read the StringIO obj line by line.
            data = stringio_data.readlines()

            #Its time to use json module now.
            json_data = list(map(json.loads, data))

            #print(json.dumps(json_data[0], indent=4, sort_keys=True))
            #send each tweet to kafka clusteri
            _thread.start_new_thread(SendtoCluster, (json_data,producer,))
            #p = Process(target=SendtoCluster, args=(json_data,producer,))
            #Pros.append(p)
            #p.start()

            
        #for t in Pros:
        #    t.join()

if __name__== "__main__":
    main()
