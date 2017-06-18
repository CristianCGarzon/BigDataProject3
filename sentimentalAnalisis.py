from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession, functions
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec
from pyspark.ml.classification import LogisticRegression
import csv
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def sentimentAnalyze():
	#rdd = sc.textFile("/ccga/SentimentAnalysisDataset.csv")
	'''#################################################FIRST DATA SET#################################################'''
	rdd = sc.textFile("/ccga/SentimentTrain60k.csv")
	r = rdd.mapPartitions(lambda x: csv.reader(x))
	parts = r.map(lambda x: Row(sentence=str.strip(x[3]), label=int(x[1])))
	spark = getSparkSessionInstance(rdd.context.getConf())
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
	lrModel.transform(final_train_data).show()
	'''#################################################FIRST DATA SET#################################################'''

	'''#################################################SECOND DATA SET#################################################'''
	rdd2 = sc.textFile("/ccga/SentimentTest40k.csv")
	r2 = rdd2.mapPartitions(lambda x: csv.reader(x))
	parts2 = r2.map(lambda x: Row(sentence=str.strip(x[3]), label=int(x[1])))
	spark2 = getSparkSessionInstance(rdd2.context.getConf())
	partsDF2 = spark.createDataFrame(parts2)
	tokenized2 = tokenizer.transform(partsDF2)
	base_words = remover.transform(tokenized2)
	train_data_raw2 = base_words.select("base_words", "label")
	word2Vec2 = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")
	model2 = word2Vec2.fit(train_data_raw2)
	final_train_data2 = model2.transform(train_data_raw2)
	final_train_data2 = final_train_data2.select("label", "features")
	predict = lrModel.transform(final_train_data2)
	predict.show()
	'''#################################################SECOND DATA SET#################################################'''
	print("-------------------------------------------Working perfect-------------------------------------------")

if __name__ == "__main__":
	sc = SparkContext(appName="sentimentalAnalisis")
	sentimentAnalyze()