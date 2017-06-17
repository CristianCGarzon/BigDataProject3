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
	rdd = sc.textFile("/ccga/Subset100k.csv")
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
	print("-------------------------------------------Working perfect-------------------------------------------")

if __name__ == "__main__":
	sc = SparkContext(appName="sentimentalAnalisis")
	sentimentAnalyze()