from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, TimestampType  
from pyspark.sql.functions import from_json, col
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException
from dotenv import load_dotenv
import os
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

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

def read_vote_data(spark: SparkSession, schema: StringType) -> DataFrame:
    
    return (
         spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', os.getenv("KAFKA_BOOTSTRAP_SERVER")) \
        .option('subscribe',os.getenv("TOPIC_NAME")) \
        .option('startingOffsets','latest') \
        .option('failOnDataLoss', 'false') \
        .load() \
        .selectExpr('CAST(value AS STRING)')\
        .select(from_json(col('value'), schema).alias('data'))\
        .select('data.*')\
        .withWatermark('timestamp', '2 minutes')\
    )
    

def write_vote_data(datafame: DataFrame, output_path: str, checkpoint_folder: str) -> StreamingQuery:
    return (
        datafame.writeStream
        .format('parquet')
        .option('checkpointLocation',checkpoint_folder)
        .option('path', output_path)
        .outputMode('append')
        .start()
    )


def main():

    load_dotenv('/opt/bitnami/spark/jobs/.env')

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
    
    # spark.sparkContext.setLogLevel('WARN')

    # Step 2: Creating the Topic in Kafka if not exists
    create_kafka_topic(
        topic_name=os.getenv("TOPIC_NAME"),
        num_partitions=1,
        replication_factor=1,
        kafka_bootstrap_server=os.getenv("KAFKA_BOOTSTRAP_SERVER")
        )

    # Step 3: Schema Definition
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('candidate', StringType(), False),
        StructField('timestamp', TimestampType(), False)
        ])

    # Step 4: Reading Streamed Vote Data
    vote_stream = read_vote_data(spark=spark, schema=schema)

    # Step 5: Writing the data to AWS S3
    query = write_vote_data(
        vote_stream, 
        f"s3a://{os.getenv('S3_BUCKET_NAME')}/checkpoints/vote_data",
        f"s3a://{os.getenv('S3_BUCKET_NAME')}/data/vote_data" 
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()

# Command to run the pyspark job
# docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 jobs/kafkaConsumerPyspark.py
