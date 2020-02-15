############################################################
# This python script is a producer for kafka.
# To send it to kafka, each csv is read into pandas DF to
# and the row by row each recorded is converted in to json
#
# 
# KafkaPorducer sends each json to kafka cluster
############################################################


from kafka import KafkaProducer
import random
import time
import pandas as pd 
import json
import os

def main():
    #read all file names in the directory
    files = os.listdir('../../home/')
    # iterate for each file name 
    for file in files:
        #read .csv file in datafarame to keep formating
        data = pd.read_csv("../../home/"+file) 
        print(data.head())
        #convert to dictionary
        dict_news=data.to_dict(orient='records')
        # kafka producer install required packages
        producer = KafkaProducer(bootstrap_servers = 'ip-10-0-0-37.us-west-2.compute.internal:9092,ip-10-0-0-12.us-west-2.compute.internal:9092,ip-10-0-0-20.us-west-2.compute.internal:9092')
        #i=0
        #send each dict to kafka cluster
        for key in dict_news:
            #i=i+1
            #if i > 3000:
            #	break
            producer.send('news',json.dumps(key))
            producer.flush()
            
if __name__ == '__main__':
    main()
