from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import pandas as pan
import matplotlib.pyplot as plot
from matplotlib import gridspec
try:
    import json
except ImportError:
    import simplejson as json
import sys
import os 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--mastern yarn --jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def graph():
    spark = SparkSession.builder.config(conf=sc.getConf()).enableHiveSupport().getOrCreate()
    queryRun = spark.sql("use bigdata")
    query= "SELECT keyword, CASE WHEN prediction = 1 THEN 'Positive' ELSE 'Negative' END AS prediction, SUM(total) AS total FROM prediction_tweets GROUP BY keyword, prediction"
    #query= "SELECT keyword, CASE WHEN prediction = 1 THEN 'Positive' ELSE 'Negative' END AS prediction, SUM(total) AS total FROM prediction_tweets_min GROUP BY keyword, prediction" #Query to run faster the graphs
    queryRun = spark.sql(query)

    dfMaga = queryRun.filter("keyword = 'maga'").toPandas()
    dfDictator = queryRun.filter("keyword = 'dictator'").toPandas()
    dfImpeach = queryRun.filter("keyword = 'impeach'").toPandas()
    dfDrain = queryRun.filter("keyword = 'drain'").toPandas()
    dfSwamp = queryRun.filter("keyword = 'swamp'").toPandas()
    dfComey = queryRun.filter("keyword = 'comey'").toPandas()

    fig = plot.figure(figsize=(8, 10))
    fig.subplots_adjust(left=0.2, top=0.85, wspace=0.6)
    gs = gridspec.GridSpec(3, 2, height_ratios=[0.5, 0.5, 0.5])
    ax0 = plot.subplot(gs[0])
    ax1 = plot.subplot(gs[1])
    ax2 = plot.subplot(gs[2])
    ax3 = plot.subplot(gs[3])
    ax4 = plot.subplot(gs[4])
    ax5 = plot.subplot(gs[5])
    
    ax0.pie(dfMaga['total'],labels=dfMaga['prediction'],shadow=False, startangle=90, autopct='%1.1f%%')
    ax0.set_title('Maga', fontsize=10)
    
    ax1.pie(dfDictator['total'],labels=dfDictator['prediction'],shadow=False, startangle=90, autopct='%1.1f%%')
    ax1.set_title('Dictator', fontsize=10)

    ax2.pie(dfImpeach['total'],labels=dfImpeach['prediction'],shadow=False, startangle=90, autopct='%1.1f%%')
    ax2.set_title('Impeach', fontsize=10)

    ax3.pie(dfDrain['total'],labels=dfDrain['prediction'],shadow=False, startangle=90, autopct='%1.1f%%')
    ax3.set_title('Drain', fontsize=10)

    ax4.pie(dfSwamp['total'],labels=dfSwamp['prediction'],shadow=False, startangle=90, autopct='%1.1f%%')
    ax4.set_title('Swamp', fontsize=10)

    ax5.pie(dfComey['total'],labels=dfComey['prediction'],shadow=False, startangle=90, autopct='%1.1f%%')
    ax5.set_title('Comey', fontsize=10)
    
    fig.canvas.set_window_title('Prediction keywords')
    fig.tight_layout()
    plot.show()

if __name__ == "__main__":
    print("Starting to graph the prediction of tweets")
    sc = SparkContext(appName="GraphPrediction")
    graph()    