from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.Builder().appName('realtime-voting-system').getOrCreate()

df = spark.readStream \
    .format('socket') \
    .option('host','localhost') \
    .option('port', 9999) \
    .load()

processed_df = df \
    .groupBy('value') \
    .agg({'value':'count'}) \
    .select("value", col('count(value)').alias('voteCount')) \

query = processed_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()