import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
from dotenv import load_dotenv
import plotly.express as px


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

def load_data(spark: SparkSession) -> DataFrame:
    df = spark.read.format('parquet') \
        .option('header','true') \
        .option('inferSchema','true') \
        .load(f"s3a://{os.getenv('S3_BUCKET_NAME')}/data/vote_data" )
    
    return df

data = load_data(spark=spark).groupby('candidate').count().toPandas()
print(data)


fig = px.pie(data, values='count', names='candidate', title='VoteShare')

st.plotly_chart(fig)



