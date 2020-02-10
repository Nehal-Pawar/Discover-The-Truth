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

table_name="mytable5"

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

    #if not str(row["topic"]): 
     #   self.dynamodb.Table(table_name).put_item(Item = { 'id':"abc", 'topic': 'topic1' })
      #  return
    if row["topic"]=="twitter":
        self.dynamodb.Table(table_name).put_item(Item = { 'id': int(row['id']), 'topic': row['topic'],'keyword':row['keyword'],'date':str(row['date']),'text':row['text'],'name':row['name'] })
    elif row["topic"]=="news": 
        self.dynamodb.Table(table_name).put_item(Item = { 'id': int(row['id']), 'topic': row['topic'],'keyword':row['keyword'],'date':str(row['date']),'title':row['title'],'publication':row['publication'],'author':row['author'],'content':row['content']})

  def close(self, err):
    # This is called after all the rows have been processed.
    if err:
      raise err

def get_dynamodb():

  access_key = aws_access_key
  secret_key = aws_secret_key

  region = "us-west-2"
  return boto3.resource('dynamodb',
                 aws_access_key_id=access_key,
                 aws_secret_access_key=secret_key,
                 region_name=region)
#def occurrencesOftopic(topic,df_twitter_5):
    #df_t_6=df_twitter_5.filter(col("text").rlike(topic)).withColumn('keyword', lit(topic))
    
    #query=df_t_6.writeStream.format("console").start()

    #query=df_t_6.writeStream.foreach(SendToDynamoDB_ForeachWriter()).outputMode("update").start()
 #   print('test2')

def main():             
    #sc = SparkContext()
    #sqlContext = SQLContext(sc)
    topic="trump"
    topics=["trump","obama","african-american"]
    spark = SparkSession.builder.appName('abc').getOrCreate()

    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "10.0.0.37:9092,10.0.0.12:9092,10.0.0.20:9092") \
      .option("subscribe", "news,twitter") \
      .load()

    df_news=df.filter(col("topic").rlike("news")) 
    df_news_1 = df_news.selectExpr("CAST(value AS STRING)","CAST(topic AS STRING)")
    df_news_2=df_news_1.select(df_news_1.topic,F.from_json(F.col("value"),"id int, title string, publication string, author string, date date, year float, month float, url string, content string").alias("json"))
        

    df_news_3=df_news_2.select(F.col("json").getItem("id").alias('id'),F.col("json").getItem("date").alias('date'),df_news_2.topic,F.col("json").getItem("title").alias('title'),F.col("json").getItem("publication").alias('publication'),F.col("json").getItem("author").alias('author'),F.col("json").getItem("year").alias('year'),F.col("json").getItem("month").alias('url'),F.col("json").getItem("content").alias('content'))
    #col_list=["id","title","publication","author","date","year","month","url","content"]
    #df_news_3=df_news_2.select([df_news_2.json[i] for i in col_list])
    #df_news_2.printSchema()

    #columns_to_drop = ['id']
    #df_news_4 = df_news_3.drop(*columns_to_drop)
    #columns=("date","content")
    #df1 = df.select(*columns)
    df_news_5=df_news_3.filter(col("json.year").rlike("2017") & (col("json.month").rlike("1") | col("json.month").rlike("2")))
#    df_news_6 = df_news_5.withColumn("keyword", lit("foo"))
        
    df_news_6=df_news_5.withColumn('keyword',F.regexp_extract('content','|'.join(topics) , 0))#.show()    
    df_news_7=df_news_6.filter(col('keyword').isin(topics))
    #df_news_6=df_news_5.filter(col("content").rlike(topic)|col("title").rlike(topic)).withColumn('keyword', lit(topic))
 
    query=df_news_7.writeStream.format("console").start()
    df_news_7.printSchema()
 
    



    df_twitter=df.filter(col("topic").rlike("twitter")) 
    df_twitter_value = df_twitter.selectExpr("CAST(value AS STRING)","CAST(topic AS STRING)")
    df_twitter_2=df_twitter_value.select(df_twitter_value.topic,F.from_json(F.col("value"),"text string,created_at string, entities string, favourite_count int, retweet_count int, user string").alias("tweet"))
    col_list_1=["text","entities","favourite_count","retweet_count","user"]

    df_t_3=df_twitter_2.select(df_twitter_2.topic,F.col("tweet").getItem("user").alias('user'),F.col("tweet").getItem("created_at").alias('date'),F.col("tweet").getItem("text").alias('text'),F.col("tweet").getItem("entities").alias('entities'),F.col("tweet").getItem("favourite_count").alias('favourite_count'),F.col("tweet").getItem("retweet_count").alias('retweet_count'))
    #df_twitter_3=df_twitter_2.select([df_twitter_2.tweet[i] for i in col_list_1])

    df_t_4=df_t_3.select(df_t_3.topic,df_t_3.date,df_t_3.text,df_t_3.favourite_count,df_t_3.retweet_count,F.from_json(F.col("user"),"id string, name string, friend_count int, verified boolean, followers_count int").alias("user"))

            
    df_twitter_5=df_t_4.select(F.col("user").getItem("id").alias('id'),df_t_4.date,df_t_4.topic, df_t_4.text,df_t_4.favourite_count,df_t_4.retweet_count,F.col("user").getItem("name").alias('name'),F.col("user").getItem("friend_count").alias('friend_count'),F.col("user").getItem("verified").alias('verified'),F.col("user").getItem("followers_count").alias('followers_count'))

    #df_twitter_5 = df_twitter_5.withColumn("keyword", lit("foo"))
    #rdd1 = sc.parallelize([topics])
    #people = rdd1.map(lambda x: Row(topic=x))
    #df_topics = sqlContext.createDataFrame(people)
    #df_topics.show()
    #schema = StructType([StructField("name", StringType(), True)])
    #df_topics=spark.createDataFrame(rdd,schema).collect()
    #df_t_6=map(lambda topic:(occurrencesOftopic(topic,df_twitter_5)),topics)
    #df_t_6=df_twitter_5.where(exists(lambda x: x > 3)(topics))
    #df_t_6=df_twitter_5.filter(col("text").isin(topics))
    
    #for topic in topics:
   # df_t_6=df_twitter_5.filter(col("text").rlike(topic)).withColumn('keyword', lit(topic))
    #df_twitter_4=df_twitter_2.select(F.col("tweet").getItem("id").alias('id'))
    #df_t_6 = reduce(lambda df_twitter_5, pat: df_twitter_5.filter(df_twitter_5['text'].rlike(pat)), ["trump","obama"], df_twitter_5)
    #df_t_6.explain()

    df_t_6=df_twitter_5.withColumn('keyword',F.regexp_extract('text',  '|'.join(topics) , 0))#.show()    
    df_t_7=df_t_6.filter(col('keyword').isin(topics))
    query=df_t_7.writeStream.format("console").option('numRows','20').option('truncate','false').start()
    df_t_7.printSchema()
     
    #print("text")    
    #spark.conf.set("spark.sql.shuffle.partitions", "1")
    query=df_news_7.writeStream.foreach(SendToDynamoDB_ForeachWriter()).outputMode("update").start()

    #query=df_t_7.writeStream.foreach(SendToDynamoDB_ForeachWriter()).outputMode("update").start()
    #dynamodb = get_dynamodb()
    #dynamodb.Table(tablename).put_item(Item = { 'id': 'fistmsg', 'topic': 'topic2' })
    query.awaitTermination()


if __name__== "__main__":
    main()
    #import os
    #user_paths = os.environ['PYTHONPATH'].split(os.pathsep)
    #print(user_paths)
#  .option("kafka.bootstrap.servers", "10.0.0.37:9092,10.0.0.12:9092,10.0.0.20:9092") \
#  .option("subscribe", "twitter") \
#  .load()
#
#
#df_t = df_t.selectExpr("CAST(value AS STRING)")
#  .option("kafka.bootstrap.servers", "10.0.0.37:9092,10.0.0.12:9092,10.0.0.20:9092") \
#  .option("subscribe", "twitter") \
#  .load()
#
#
#df_t = df_t.selectExpr("CAST(value AS STRING)")
#query=df_t.writeStream.format("console").start()
#df_t.printSchema()
#df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#print 'Event recieved in window: '
#kafkaStream.pprint()
#kafkaSteam.print()

#df.isStreaming()
#ssc.start()
#ssc.awaitTermination()
#df=df.select(F.col("json").getItem("no"),F.col("json").getItem("id").alias('id'))
#df=df.select("value")
#df=df.flatMap(parseLog).select("value")

#new_df=split_col = split(df['value'], ',')
#print(type(new_df))
#words = df.select(explode(split(",")))
#df = df.withColumn('Splitted', split(df['Value'], '\,'))
# df1 = df.withColumn('Splitted', F.split(df['Value'], '\,'))\
# .withColumn('no',F.col('Splitted')[0]).withColumn('id', F.col('Splitted')[1])\
# .withColumn('title', F.col('Splitted')[2]).withColumn('publication', F.col('Splitted')[3])\
# .withColumn('author', F.col('Splitted')[4]).withColumn('date', F.col('Splitted')[5])\
# .withColumn('year', F.col('Splitted')[6]).withColumn('month', F.col('Splitted')[7])\
# .withColumn('url', F.col('Splitted')[8]).withColumn('content', F.col('Splitted')[9])
#df4=spark.read.json(df)
#new_df = sqlcontext.read.json(df.map(lambda r: r.json))
#df2=json.loads(df)
#conf = (SparkConf()).setMaster("spark://10.0.0.9:7077")

#sc = SparkContext(conf = conf)
#ssc = StreamingContext(sc, 5)
#kafkaStream = KafkaUtils.createStream(ssc, "ip-10-0-0-37.us-west-2.compute.internal:2181", "consumer-group", {"abc": 1})

#new1=df.withColumn("_tmp", split($"value", ",")).select(
#  $"_tmp".getItem(0).as("col1"),
#  $"_tmp".getItem(1).as("col2"),
#  $"_tmp".getItem(2).as("col3"),
#  $"_tmp".getItem(3).as("col4"),
 # $"_tmp".getItem(4).as("col5")
  #$"_tmp".getItem(5).as("col6")

#).drop("_tmp")

#print(words.columns)
#new = df.split(",", n = 1, expand = True) 
