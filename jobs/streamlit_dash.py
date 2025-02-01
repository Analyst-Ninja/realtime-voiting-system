import os 
import streamlit as st
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql import DataFrame


def main():

    spark = SparkSession.Builder().appName('streamlit_dash') \
            .config('spark.jars.packages',
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469"
            ) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))\
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))\
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
            .getOrCreate()
    
    st.title("📊 Real-Time Streaming Dashboard")

    # Function to read S3 data
    def load_data(spark: SparkSession, schema: StructType) -> DataFrame:
        df = spark.read.format('parquet') \
            .option('header','true') \
            .option('inferSchema','true') \
            .load(f"s3a://{os.getenv('S3_BUCKET_NAME')}/data/vote_data" )
        
        return df.groupBy('candidate').count().alias('voteCount').toPandas()

    # Auto-refresh mechanism
    placeholder = st.empty()

    while True:
        df = load_data()
        
        if not df.empty:
            with placeholder.container():
                st.subheader("Latest Data")
                st.dataframe(df)

                st.subheader("Live Value Plot")
                # st.line_chart(data=df, x='id', y='value')

        time.sleep(10)  # Refresh every 2 seconds

if __name__ == '__main__':
    main()