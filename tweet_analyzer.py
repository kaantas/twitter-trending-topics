# RUNNING >> PYSPARK_PYTHON=ipython3 ../spark/spark-2.1.1-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1 

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.streaming.kafka import KafkaUtils
import json
from collections import namedtuple

if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
	sc = SparkContext(appName="TwitterTrendAnalyzer")

	#Set the Batch Interval is 10 sec of Streaming Context
	ssc = StreamingContext(sc, 10)

	# Query with SQL
	#sqlContext = SQLContext(sc)
	#sqlContext = SparkSession.builder.appName("TwitterTrendAnalyzer").config("spark.driver.allowMultipleContexts", "true").getOrCreate()
	
	#Create Kafka Stream to Consume Data Comes From Twitter Topic
	#localhost:2181 = Default Zookeeper Consumer Address
	kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'kafka-spark-streaming', {'twitter':1})
    
	#Parse Twitter Data as json
	parsed = kafkaStream.map(lambda v: json.loads(v[1]))

	words = parsed.map(lambda tweet: tweet['text']).flatMap(lambda x: x.split(" "))

	hashtags = words.filter(lambda word: word.lower().startswith("#"))

	count_hashtags = hashtags.map(lambda word: (word.lower(), 1)).reduceByKey(lambda x, y: x+y)

	#count_hashtags.pprint()

	columns = ("tag", "count")
	tweet_tuple = namedtuple('Tweet', columns)

	mapped_tuple = count_hashtags.map(lambda data: tweet_tuple(data[0], data[1]))

	#mapped_tuple.pprint()
	
	mapped_tuple.foreachRDD(lambda rdd: rdd.toDF().sort(desc("count")).limit(10).registerTempTables("tweets").show())

	#sqlContext.sql("select tag, count from tweets").show()
	#Count the number of tweets per User
    #user_counts = parsed.map(lambda tweet: (tweet['user']["screen_name"], 1)).reduceByKey(lambda x,y: x + y)

	#Print the User tweet counts
    #user_counts.pprint()

	#Start Execution of Streams
	ssc.start()
	
	ssc.awaitTermination()
