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
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    context = StreamingContext(sc, 60)
    dStream = KafkaUtils.createDirectStream(context, ["project3"], {"metadata.broker.list": "localhost:9092"})
    dStream.foreachRDD(prediction)
    context.start()
    context.awaitTermination()

def trainData():
    #rdd = sc.parallelize(rdd)
    #rdd.foreach(print)
    #rdd = sc.textFile("/ccga/SentimentAnalysisDataset.csv")
    '''#################################################TRAINING DATA SET#################################################'''
    rddTrain = sc.textFile("/ccga/set100k.csv")
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
    return lrModel
    '''#################################################TRAINING DATA SET#################################################'''

def prediction(time,rdd):
    rdd2 = rdd.map(lambda x: json.loads(x[1]))
    rdd2 = rdd2.map(lambda x: str.strip(x["text"])).map(lambda x: x.lower())
    rdd2 = rdd2.map(lambda x: x.replace('.','')).map(lambda x: x.replace('(','')).map(lambda x: x.replace(')','')).map(lambda x: x.replace('!','')).map(lambda x: x.replace('|','')).map(lambda x: x.replace('!',''))
    rdd2 = rdd2.map(lambda x: x.replace('...','')).map(lambda x: x.replace('?','')).map(lambda x: x.replace(';','')).map(lambda x: x.replace('"','')).map(lambda x: x.replace('/','')).map(lambda x: x.replace(',',''))
    rdd2 = rdd2.map(lambda x: x.replace('~','')).map(lambda x: x.replace('{','')).map(lambda x: x.replace('}','')).map(lambda x: x.replace('-','')).map(lambda x: x.replace('`',''))
    rdd2 = rdd2.map(lambda x: ' '.join(filter(lambda x: x.startswith(('@','http','"','&','rt')) == False, x.split())))

    maga = rdd2.filter(lambda x: "maga" in x).map(lambda x: [x, "maga"])
    dictator = rdd2.filter(lambda x: "dictator" in x).map(lambda x: [x, "dictator"])
    impeach = rdd2.filter(lambda x: "impeach" in x).map(lambda x: [x, "impeach"])
    drain = rdd2.filter(lambda x: "drain" in x).map(lambda x: [x, "drain"])
    swamp = rdd2.filter(lambda x: "swamp" in x).map(lambda x: [x, "swamp"])
    comey = rdd2.filter(lambda x: "comey" in x).map(lambda x: [x, "comey"])
    rdd3 = maga.union(dictator).union(impeach).union(drain).union(swamp).union(comey)

    parts2 = rdd3.map(lambda x: Row(text=x[0], keyword=x[1], dateTime = time))
    spark2 = getSparkSessionInstance(rdd2.context.getConf())
    partsDF2 = spark2.createDataFrame(parts2)
    tokenizer2 = Tokenizer(inputCol="text", outputCol="words")
    tokenized2 = tokenizer2.transform(partsDF2)
    remover2 = StopWordsRemover(inputCol="words", outputCol="base_words")
    base_words = remover2.transform(tokenized2)
    train_data_raw2 = base_words.select("base_words", "text", "keyword", "dateTime")
    word2Vec2 = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")
    model2 = word2Vec2.fit(train_data_raw2)
    final_train_data2 = model2.transform(train_data_raw2)
    final_train_data2 = final_train_data2.select("text", "features", "keyword", "dateTime")
    predict = lrModel.transform(final_train_data2)
    predict.show()
    predict = predict.createOrReplaceTempView("prediction_tweets")
    predict = spark2.sql("use bigdata")
    predict = spark2.sql("select keyword, prediction, dateTime, count(*) as total from prediction_tweets group by keyword, prediction, dateTime")
    predict.write.mode("append").saveAsTable("prediction_tweets")
    print("Ipredictnserted in Prediction table FINISH TIME: ", time)

    #predict.filter("prediction = 0").show()
    '''#################################################SECOND DATA SET#################################################'''
    print("-------------------------------------------Working perfect-------------------------------------------")

if __name__ == "__main__":
    print("Starting to read tweets")
    sc = SparkContext(appName="ConsumerProject3")
    lrModel = trainData()
    consumer()