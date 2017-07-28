from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":

	spark = SparkSession.builder.appName("KafkaSparkStructuredStreaming").getOrCreate()

	kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").load()

	kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING)")

	tweets_table = kafka_df_string.alias("tweets")

	grouped_by_hashtags = tweets_table.groupBy("value").count().orderBy(desc("count"))

	query = grouped_by_hashtags.writeStream.outputMode("complete").format("console").start()

	query.awaitTermination()
