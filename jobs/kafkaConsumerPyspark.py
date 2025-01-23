from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.Builder().appName('Kafka-Consumer') \
    .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .getOrCreate()

kafka_stream = spark \
                .readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subscribe','voteTopic') \
                .option('startingOffsets','latest') \
                .load()

kafka_values = kafka_stream.selectExpr('CAST(value AS STRING) AS message')

query = kafka_values.groupBy('message').count().orderBy(F.col('count').desc()) \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

query.awaitTermination()