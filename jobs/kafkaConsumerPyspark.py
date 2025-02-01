from pyspark.sql import SparkSession
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException
from dotenv import load_dotenv
import os

def create_kafka_topic(topic_name: str, 
                       num_partitions: int, 
                       replication_factor: int,
                       kafka_bootstrap_server: str
                       ) -> None:
    
    config = {'bootstrap.servers': kafka_bootstrap_server}

    admin_client = AdminClient(config)

    try:
        metadata = admin_client.list_topics(timeout=10)
        
        if topic_name not in metadata.topics:
            print(f"Topic {topic_name} does not exist. Creating it now.")

            new_topic = NewTopic(topic_name, num_partitions, replication_factor)
            admin_client.create_topics([new_topic])
            print(f"Topic {topic_name} created.")
        else:
            print(f"Topic {topic_name} already exists.")

    except KafkaException as e:
        print(f"An error occurred: {e}")

def read_vote_data(spark: SparkSession):
    pass


def main():

    load_dotenv()

    # Step 1: Creating Spark Session 
    spark = SparkSession.Builder().appName('rt-voting-system') \
        .config('spark.jars.packages',
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469"
                ) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))\
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')

    # Step 2: Creating the Topic in Kafka if not exists
    create_kafka_topic(
        topic_name=os.getenv("TOPIC_NAME"),
        num_partitions=os.getenv("NUM_OF_PARITIONS"),
        replication_factor=os.getenv("REPLICATION_FACTOR"),
        kafka_bootstrap_server=os.getenv("KAFKA_BOOTSTRAP_SERVER")
        )

    # Step 3: Reading Streamed Vote Data
    kafka_stream = spark \
                    .readStream \
                    .format('kafka') \
                    .option('kafka.bootstrap.servers', os.getenv("KAFKA_BOOTSTRAP_SERVER")) \
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

if __name__ == "__main__":
    main()