
import pandas as pd
import os
import sys
import boto3
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , DoubleType ,\
                                StringType , IntegerType , DateType ,\
                                TimestampType,DecimalType,LongType


from pyspark import SparkConf,SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import concat,concat_ws,col,lit
from pyspark.sql.functions import col, year, month, dayofweek, hour, weekofyear, dayofmonth,monotonically_increasing_id
from pyspark.sql.functions import expr, col
from pyspark.sql.functions import when
from pyspark.sql.functions import max
from pyspark.sql.window import Window

import configparser
from pyspark.sql import Row

import json
import time

from common_modules import create_spark_session
from common_modules import aws_client
from common_modules import refresh_parquet_files

def create_tmp_dimensional_tables_tweets(dl_bucket_input_prefix, spark):
    """
    This function creates 4 dataframes from tickers
    tweets related data fetched from input folder.
    :param dl_bucket_input_prefix: prefix of the input folder
    :param spark: spark object
    :return: tmp_tweets_dim,tmp_user_id_dim,tmp_hashtags_dim,tmp_tickers_tweets_users_fact
    """
    #input path
    tweets_path = os.path.join(dl_bucket_input_prefix,'tweets/')
#     tickers_cashflow_path = os.path.join(markets_static_data_path,'tickers_cashflow/')
    tweets_json_path = os.path.join(tweets_path,'*.json')
    
    #input df
    tweets_sp_df = spark.read.option("multiline","true") \
      .json(tweets_json_path)
        
    #tmp_tweets_dim
    tmp_tweets_dim = tweets_sp_df.select('tweet_id','created_at','lang','source','source_url','text',\
                                'in_reply_to_status_id','truncated').dropDuplicates()
    
    #tmp_user_id_dim
    user_window = Window \
        .partitionBy('user_id') \

    tmp_user_id_dim = tweets_sp_df.select('user_id','user_created_at','user_description','user_followers_count',\
                                      'friends_count','user_location','user_name','user_verified' ).\
                                withColumn('user_followers_count', max(col('user_followers_count')).\
                                            over(user_window)).\
                                withColumn('friends_count', max(col('friends_count')).\
                                            over(user_window)).dropDuplicates()

    #tmp_hashtags_dim
    hashtags_ls = tweets_sp_df.select('hashtags','tweet_id').dropDuplicates().collect()
    # hastags_ls#['hashtags']

    rows_ls = []
    for row in hashtags_ls:
    #     row = []
        for i in row.hashtags:
            rows_ls.append((i,row.tweet_id))
    # rows_ls
    columns = ['hashtag','tweet_id']
    tmp_hashtags_dim = spark.createDataFrame(data=rows_ls,schema=columns).dropDuplicates().\
        withColumn("hashtag_id", F.monotonically_increasing_id())
    
    #tmp_tickers_tweets_users_fact
    tmp_tickers_tweets_users_fact = tweets_sp_df.select('tweet_id','user_id','ticker').dropDuplicates()
    
    #tmp_tickers_tweets_users_fact
    return tmp_tweets_dim,tmp_user_id_dim,tmp_hashtags_dim,tmp_tickers_tweets_users_fact

def main():

    config = configparser.ConfigParser()    
    config.read('/home/hadoop/dl_config.cfg')  
    
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    
    aws_access_key_id = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    region = config.get('CLUSTER', 'AWS_REGION')
    
    DL_BUCKET_NAME = config.get('S3_BUCKET','DL_BUCKET_NAME')
    DL_BUCKET_INPUT_PREFIX = config.get('S3_BUCKET','DL_BUCKET_INPUT_PREFIX')
    DL_BUCKET_OUTPUT_PREFIX = config.get('S3_BUCKET','DL_BUCKET_OUTPUT_PREFIX')

    print('python version : ', sys.version)

    print("create spark session")   
    spark = create_spark_session()
    
    # spark version
    print(f"Spark version = {spark.version}")

    # hadoop version
    sparkContext=spark.sparkContext
    print(f"Hadoop version = {sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")  
    
    print('create s3 client')
    s3_client = aws_client(s3_service = 's3', region = region, access_key_id = aws_access_key_id,\
                          secret_access_key = aws_secret_access_key)

    print('create dimensional tables from imported data')
    tmp_tweets_dim,tmp_user_id_dim,tmp_hashtags_dim,tmp_tickers_tweets_users_fact =\
        create_tmp_dimensional_tables_tweets(dl_bucket_input_prefix = DL_BUCKET_INPUT_PREFIX,\
                                          spark = spark)

    print('tmp_tweets_dim')
    print(tmp_tweets_dim.show(5))
    
    #output path
    tweets_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'tweets_dim/')
    write_files = os.path.join(tweets_dim_path,'tweets_dim.parquet')
    read_files = os.path.join(tweets_dim_path,'tweets_dim_tmp.parquet')
    
    print('update tweets_dim in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_tweets_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/tweets_dim/tweets_dim.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='tweets_dim',
                           columns_list=['tweet_id']
                          )

    print('tmp_user_id_dim')
    print(tmp_user_id_dim.show(5))
    
    #output path
    user_id_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'user_id_dim/')
    write_files = os.path.join(user_id_dim_path,'user_id_dim.parquet')
    read_files = os.path.join(user_id_dim_path,'user_id_dim_tmp.parquet')
    
    print('update user_id_dim in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_user_id_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/user_id_dim/user_id_dim.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='user_id_dim',
                           columns_list=['user_id']
                          )
    
    print('tmp_hashtags_dim')
    print(tmp_hashtags_dim.show(5))
    
    #output path
    hashtags_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'hashtags_dim/')
    write_files = os.path.join(hashtags_dim_path,'hashtags_dim.parquet')
    read_files = os.path.join(hashtags_dim_path,'hashtags_dim_tmp.parquet')
    
    print('update hashtags_dim in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_hashtags_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/hashtags_dim/hashtags_dim.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='hashtags_dim',
                           columns_list=['hashtag_id','tweet_id','hashtag']
                          )

    print('tmp_tickers_tweets_users_fact')
    print(tmp_tickers_tweets_users_fact.show(5))
    
    #output path
    tickers_tweets_users_fact_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'tickers_tweets_users_fact/')
    write_files = os.path.join(tickers_tweets_users_fact_path,'tickers_tweets_users_fact.parquet')
    read_files = os.path.join(tickers_tweets_users_fact_path,'tickers_tweets_users_fact_tmp.parquet')
    
    print('update tickers_tweets_users_fact in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_tickers_tweets_users_fact,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/tickers_tweets_users_fact/tickers_tweets_users_fact.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='tickers_tweets_users_fact',
                           columns_list=['ticker','user_id','tweet_id']
                          )
    
    
if __name__ == "__main__":
    main()
