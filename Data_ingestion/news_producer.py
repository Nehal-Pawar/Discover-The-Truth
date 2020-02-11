############################################################
# This python script is a producer for kafka.
# To send it to kafka, each record is first converted to
# string then to bytes using str.encode('utf-8') method.
#
# The parameters
# config.KAFKA_SERVERS: public DNS and port of the servers
# were written in a separate "config.py".
############################################################


from kafka import KafkaProducer
import random
import time
import pandas as pd 
import json
import os
def main():
    entries = os.listdir('../../home/')
    for entry in entries:
        data = pd.read_csv("../../home/"+entry) 
        print(data.head())
        dict_news=data.to_dict(orient='records')
        
        producer = KafkaProducer(bootstrap_servers = 'ip-10-0-0-37.us-west-2.compute.internal:9092,ip-10-0-0-12.us-west-2.compute.internal:9092,.internal:9092,ip-10-0-0-20.us-west-2.compute.internal:9092')
        #i=0
        for key in dict_news:
            #i=i+1
            #if i > 3000:
            #	break
            producer.send('news',json.dumps(key))
            producer.flush()
            
if __name__ == '__main__':

    main()
