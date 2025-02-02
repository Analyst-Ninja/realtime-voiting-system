import os 
import streamlit as st
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from dotenv import load_dotenv
from pyspark.sql import functions as F
import plotly.express as px
import uuid

def main():
    load_dotenv()

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
    def load_data(spark: SparkSession) -> DataFrame:
        df = spark.read.format('parquet') \
            .option('header','true') \
            .option('inferSchema','true') \
            .load(f"s3a://{os.getenv('S3_BUCKET_NAME')}/data/vote_data" )
        
        return df.groupBy('candidate').count().orderBy(F.col('count').desc()).toPandas()

    # Auto-refresh mechanism
    placeholder = st.empty()

    while True:
        df = load_data(spark=spark)
        print(df)
        
        if not df.empty:
            with placeholder.container():
                st.subheader("Latest Data")
                st.dataframe(df)

                st.subheader("Live Value Plot")
                # st.line_chart(data=df, x='id', y='value')
                # st.bar_chart(data=df, x=df['candidate'], y=df['id'])
                fig = px.pie(df, values='count', names='candidate', title='VoteShare')
                st.plotly_chart(fig, key=str(uuid.uuid4()))


        time.sleep(10)  # Refresh every 10 seconds

if __name__ == '__main__':
    main()