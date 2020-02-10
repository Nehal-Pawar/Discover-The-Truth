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

def main():
    data = pd.read_csv("/all_news_DB/home/articles1.csv") 
    print(data.head())
    dict_news=data.to_dict(orient='records')
    #msg1=json.dumps(dict_news)
    producer = KafkaProducer(bootstrap_servers = 'ip-10-0-0-37.us-west-2.compute.internal:9092,ip-10-0-0-12.us-west-2.compute.internal:9092,.internal:9092,ip-10-0-0-20.us-west-2.compute.internal:9092')
    #producer.send('news',msg1)
    i=0
    for key in dict_news:
	i=i+1
	#if i > 3000:
	#	break
	producer.send('news',json.dumps(key))
	#print(len(key))
	#print(key)
        producer.flush()
	
if __name__ == '__main__':

    main()
