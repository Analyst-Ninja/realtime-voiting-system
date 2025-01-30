from pyspark.sql import SparkSession
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException


spark = SparkSession.Builder().appName('Kafka-Consumer') \
    .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .getOrCreate()

# TODO: Topic Creation if not exists

# Kafka configuration
config = {'bootstrap.servers': 'broker:29092'}

# Create AdminClient
admin_client = AdminClient(config)

# Define the topic
topic_name = "voteTopic"
num_partitions = 1
replication_factor = 1

# Check if the topic exists
try:
    # Fetch metadata for the topic
    metadata = admin_client.list_topics(timeout=10)
    
    if topic_name not in metadata.topics:
        print(f"Topic {topic_name} does not exist. Creating it now.")
        
        # Create the topic
        new_topic = NewTopic(topic_name, num_partitions, replication_factor)
        admin_client.create_topics([new_topic])
        print(f"Topic {topic_name} created.")
    else:
        print(f"Topic {topic_name} already exists.")

except KafkaException as e:
    print(f"An error occurred: {e}")

kafka_stream = spark \
                .readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'broker:29092') \
                .option('subscribe','voteTopic') \
                .option('startingOffsets','latest') \
                .load()

kafka_values = kafka_stream.selectExpr(
    'CAST(value AS STRING) AS candidate', 'current_timestamp() as timestamp'
    )

query = kafka_values \
        .withWatermark("timestamp", "1 minutes") \
        .writeStream \
        .outputMode('append') \
        .format('parquet') \
        .option('path', 'stream_data/data') \
        .option('checkpointLocation', 'checkpoint/data') \
        .option('header', True) \
        .trigger(processingTime='10 seconds') \
        .start()

query.awaitTermination()