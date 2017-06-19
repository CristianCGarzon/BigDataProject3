from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pyspark.sql import Row, SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession, functions
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec
from pyspark.ml.classification import LogisticRegression
import csv
try:
    import json
except ImportError:
    import simplejson as json
import os 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    context = StreamingContext(sc, 30)
    dStream = KafkaUtils.createDirectStream(context, ["project3"], {"metadata.broker.list": "localhost:9092"})
    dStream.foreachRDD(prediction)
    context.start()
    context.awaitTermination()

def insertUsers(users, spark, time):
    if users:
        rddUsers = sc.parallelize(users)
        # Convert RDD[String] to RDD[Row] to DataFrame
        usersDataFrame = spark.createDataFrame(rddUsers.map(lambda x: Row(screenName=x, hora=time)))
        usersDataFrame.createOrReplaceTempView("users")
        usersDataFrame = spark.sql("use bigdata")
        usersDataFrame = spark.sql("select screenName, hora from users limit 50")
        usersDataFrame.write.mode("append").saveAsTable("users")
        print("Inserted Users FINISH")
    else:
        print("Is not Users avaliables to insert in hive")

def prediction(time,rdd):
    rdd = rdd.map(lambda x: json.loads(x[1]))
    rdd = rdd.collect()
    rdd = sc.parallelize(rdd)
    rdd.foreach(print)
    #rdd = sc.textFile("/ccga/SentimentAnalysisDataset.csv")
    '''#################################################TRAINING DATA SET#################################################'''
    '''rddTrain = sc.textFile("/ccga/SentimentTrain60k.csv")
    r = rddTrain.mapPartitions(lambda x: csv.reader(x))
    parts = r.map(lambda x: Row(sentence=str.strip(x[3]), label=int(x[1])))
    spark = getSparkSessionInstance(rddTrain.context.getConf())
    partsDF = spark.createDataFrame(parts)
    #partsDF.show(truncate=False)
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    tokenized = tokenizer.transform(partsDF)
    #okenized.show(truncate=False)
    remover = StopWordsRemover(inputCol="words", outputCol="base_words")
    base_words = remover.transform(tokenized)
    #base_words.show(truncate=False)
    train_data_raw = base_words.select("base_words", "label")
    #train_data_raw.show(truncate=False)
    #base_words = train_data_raw.select("base_words")
    #base_words_rdd = base_words.rdd
    #print(base_words_rdd.collect())
    #base_words_map = base_words_rdd.flatMap(lambda x: x[0])
    #base_words_rdd.collect()
    word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")
    model = word2Vec.fit(train_data_raw)
    final_train_data = model.transform(train_data_raw)
    #final_train_data.show()
    final_train_data = final_train_data.select("label", "features")
    #final_train_data.show(truncate=False)
    lr = LogisticRegression(maxIter=1000, regParam=0.001, elasticNetParam=0.0001)
    lrModel = lr.fit(final_train_data)
    trained = lrModel.transform(final_train_data)
    '''#################################################TRAINING DATA SET#################################################'''

    '''rdd2 = rdd.map(lambda x: json.loads(x[1]))
    rdd2 = rdd2.collect()
    rdd2 = sc.parallelize(rdd2)

    #rdd2 = sc.textFile("/ccga/SentimentTest40k.csv")
    #r2 = rdd2.mapPartitions(lambda x: csv.reader(x))
    parts2 = rdd2.map(lambda x: Row(text=x["text"].lower(), palabra="prueba"))
    spark2 = getSparkSessionInstance(rdd2.context.getConf())
    partsDF2 = spark2.createDataFrame(parts2)
    tokenizer2 = Tokenizer(inputCol="sentence", outputCol="words")
    tokenized2 = tokenizer2.transform(partsDF2)
    remover2 = StopWordsRemover(inputCol="words", outputCol="base_words")
    base_words = remover2.transform(tokenized2)
    train_data_raw2 = base_words.select("base_words", "text", "palabra")
    final_train_data2 = model.transform(train_data_raw2)
    final_train_data2 = final_train_data2.select("text", "features", "palabra")
    predict = lrModel.transform(final_train_data2)
    #trained.show()
    predict.show()
    '''#################################################SECOND DATA SET#################################################'''
    print("-------------------------------------------Working perfect-------------------------------------------")

if __name__ == "__main__":
    print("Starting to read tweets")
    sc = SparkContext(appName="Consumer")
    consumer()