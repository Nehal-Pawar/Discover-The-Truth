############################################################
# This python script is a spark streaming comsumer.
# comming from kafka, 2 topics news and twitter
# 
#
# parameter
# config.py file to get AWS access key and secret keys
# each streaming row from data frame is send to dynamoDB
############################################################

from pyspark import SparkConf, SparkContext
#from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
import json
from pyspark.sql import Row
import boto3
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

import sys
sys.path.insert(1, '/home/ubuntu/')
from config import (aws_access_key, aws_secret_key)

table_name="mytable6"

class SendToDynamoDB_ForeachWriter:
  '''
  Class to send a set of rows to DynamoDB.
  When used with `foreach`, copies of this class is going to be used to write
  multiple rows in the executor. See the python docs for `DataStreamWriter.foreach`
  for more details.
  '''

  def open(self, partition_id, epoch_id):
    # This is called first when preparing to send multiple rows.
    # Put all the initialization code inside open() so that a fresh
    # copy of this class is initialized in the executor where open()
    # will be called.
    self.dynamodb = get_dynamodb()
    return True

  def process(self, row):
    # This is called for each row after open() has been called.
    # This implementation sends one row at a time.
    # A more efficient implementation can be to send batches of rows at a time.

    if row["topic"]=="twitter":
        self.dynamodb.Table(table_name).put_item(Item = { 'id': int(row['id']), 'topic': row['topic'],'keyword':row['keyword'],'date':str(row['date']),'text':row['text'],'name':row['name'] })
    elif row["topic"]=="news": 
        self.dynamodb.Table(table_name).put_item(Item = { 'id': int(row['id']), 'topic': row['topic'],'keyword':row['keyword'],'date':str(row['date']),'title':row['title'],'publication':row['publication'],'author':row['author'],'content':row['content']})

  def close(self, err):
    # This is called after all the rows have been processed.
    if err:
      raise err

def get_dynamodb():
  '''
  Class to connect to DynamoDB.
  It access teh access key and secret key and conects to resource 
  dynamoDB returning boto3 object 
  which gets used i open methid of class SendToDynamoDB_ForeachWriter
  '''
  access_key = aws_access_key
  secret_key = aws_secret_key

  region = "us-west-2"
  return boto3.resource('dynamodb',
                 aws_access_key_id=access_key,
                 aws_secret_access_key=secret_key,
                 region_name=region)

def main():             
    # news topics acting as keyword 
    topics=["2016 presidential election","gunman opens fire","U.S.-Mexico border","refugees of the Syrian Civil War","Syrian Civil War","African-American","45th President","Chelsea Manning's"]
    spark = SparkSession.builder.appName('abc').getOrCreate()
    # reading data from kafka 
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "10.0.0.37:9092,10.0.0.12:9092,10.0.0.20:9092") \
      .option("auto.offset.reset","earliest")\
      .option("subscribe", "news,twitter") \
      .load()
    
    
    # filtering news topic from kafka stream
    df_news=df.filter(col("topic").rlike("news"))
    # casting columns to string 
    df_news_1 = df_news.selectExpr("CAST(value AS STRING)","CAST(topic AS STRING)")
    # convert json column value to multiple columns using pyspark functions
    df_news_2=df_news_1.select(df_news_1.topic,F.from_json(F.col("value"),"id int, title string, publication string, author string, date date, year float, month float, url string, content string").alias("json"))
    # create new Dataframe out of the value column
    df_news_3=df_news_2.select(F.col("json").getItem("id").alias('id'),F.col("json").getItem("date").alias('date'),df_news_2.topic,F.col("json").getItem("title").alias('title'),F.col("json").getItem("publication").alias('publication'),F.col("json").getItem("author").alias('author'),F.col("json").getItem("year").alias('year'),F.col("json").getItem("month").alias('url'),F.col("json").getItem("content").alias('content'))
    # filter news for january and feb month
    df_news_5=df_news_3.filter(col("json.year").rlike("2017") & (col("json.month").rlike("1") | col("json.month").rlike("2")))
    # tag rows with keyword that match regular expression defined by topic in list
    df_news_6=df_news_5.withColumn('keyword',F.regexp_extract('content','|'.join(topics) , 0))
    # only keep rows that are tagged by some topics
    df_news_7=df_news_6.filter(col('keyword').isin(topics))
    # stream the result to output console
    query=df_news_7.writeStream.format("console").start()
    df_news_7.printSchema()
  
    #########Twitter###########

    #filtering twitter topic from kafka stream
    df_twitter=df.filter(col("topic").rlike("twitter")) 
    # casting columns to string 
    df_twitter_value = df_twitter.selectExpr("CAST(value AS STRING)","CAST(topic AS STRING)")
    # convert json column value to multiple columns using pyspark functions
    df_twitter_2=df_twitter_value.select(df_twitter_value.topic,F.from_json(F.col("value"),"text string,created_at string, entities string, favourite_count int, retweet_count int, user string").alias("tweet"))
    #col_list_1=["text","entities","favourite_count","retweet_count","user"]
    # create new Dataframe out of the value column
    df_t_3=df_twitter_2.select(df_twitter_2.topic,F.col("tweet").getItem("user").alias('user'),F.col("tweet").getItem("created_at").alias('date'),F.col("tweet").getItem("text").alias('text'),F.col("tweet").getItem("entities").alias('entities'),F.col("tweet").getItem("favourite_count").alias('favourite_count'),F.col("tweet").getItem("retweet_count").alias('retweet_count'))
    # level 2 json to column
    df_t_4=df_t_3.select(df_t_3.topic,df_t_3.date,df_t_3.text,df_t_3.favourite_count,df_t_3.retweet_count,F.from_json(F.col("user"),"id string, name string, friend_count int, verified boolean, followers_count int").alias("user"))
    # create new Dataframe out of the value column            
    df_twitter_5=df_t_4.select(F.col("user").getItem("id").alias('id'),df_t_4.date,df_t_4.topic, df_t_4.text,df_t_4.favourite_count,df_t_4.retweet_count,F.col("user").getItem("name").alias('name'),F.col("user").getItem("friend_count").alias('friend_count'),F.col("user").getItem("verified").alias('verified'),F.col("user").getItem("followers_count").alias('followers_count'))
    # tag rows with keyword that match regular expression defined by topic in list
    df_t_6=df_twitter_5.withColumn('keyword',F.regexp_extract('text',  '|'.join(topics) , 0))
    # only keep rows that are tagged by some topics
    df_t_7=df_t_6.filter(col('keyword').isin(topics))
    # stream the result to output console
    query=df_t_7.writeStream.format("console").option('numRows','20').option('truncate','false').start()
    df_t_7.printSchema()
    # stream output Dataframe to DynamoDB News
    query=df_news_7.writeStream.foreach(SendToDynamoDB_ForeachWriter()).outputMode("update").start()
    # stream output Dataframe to DynamoDB Twitter
    query=df_t_7.writeStream.foreach(SendToDynamoDB_ForeachWriter()).outputMode("update").start()
    query.awaitTermination()


if __name__== "__main__":
    main()
   
