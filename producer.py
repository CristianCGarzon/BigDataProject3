from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient, KafkaProducer
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
import json
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def read_credentials():
    file_name = "/home/garzoncristianc/credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load "+data_file)
        return None

def producer1():
    sc = SparkContext(appName="ProducerProject3")
    ssc = StreamingContext(sc, 120)
    kvs = KafkaUtils.createDirectStream(ssc, ["test"], {"metadata.broker.list": "localhost:9092"})
    kvs.foreachRDD(send)
    producer.flush()
    ssc.start()
    ssc.awaitTermination()

def send(message):
    stream = twitter_stream.statuses.filter(track="MAGA,DICTATOR,IMPEACH,DRAIN,SWAMP,COMEY", language="en")
    count=0
    for tweet in stream:
        producer.send('project3', bytes(json.dumps(tweet, indent=6), "ascii"))
        count+=1
        if(count==4000):
            break

if __name__ == "__main__":
    print("Starting to read tweets")
    credentials = read_credentials()
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer1()
