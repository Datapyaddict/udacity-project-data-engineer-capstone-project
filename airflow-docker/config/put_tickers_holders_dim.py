
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

def create_tmp_dimensional_tables_holders(dl_bucket_input_prefix, spark):
    """
    This function creates 2 dataframes (tmp_holders_dim,tmp_tickers_holders_fact) from tickers
    holders related data fetched from input folder.
    :param dl_bucket_input_prefix: prefix of the input folder
    :param spark: spark object
    :return: tmp_financial_report_dim,tmp_tickers_financials_fact
    """
    # input path
    markets_static_data_path = os.path.join(dl_bucket_input_prefix,'markets_static_data/')
    tickers_holders_path = os.path.join(markets_static_data_path,'tickers_holders/')
    tickers_holders_parquet_path = os.path.join(tickers_holders_path,'tickers_holders.parquet')
    
    # input df
    tickers_holders_sp_df = spark.read.parquet(tickers_holders_parquet_path) 
        
    # tmp_holders_dim
    tmp_holders_dim = tickers_holders_sp_df.select('holder','holder_type').distinct()
    
    #tmp_tickers_holders_fact
    tmp_tickers_holders_fact = tickers_holders_sp_df.\
        select(col('symbol').alias('ticker'),'holder','shares','date_reported','out', 'value' )

    return tmp_holders_dim,tmp_tickers_holders_fact

    
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
    tmp_holders_dim,tmp_tickers_holders_fact =\
        create_tmp_dimensional_tables_holders(dl_bucket_input_prefix = DL_BUCKET_INPUT_PREFIX,\
                                          spark = spark)

    print('tmp_holders_dim')
    print(tmp_holders_dim.show(5))
    
    #output path
    holders_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'holders_dim/')
    write_files = os.path.join(holders_dim_path,'holders_dim.parquet')
    read_files = os.path.join(holders_dim_path,'holders_dim_tmp.parquet')
    
    print('update holders_dim in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_holders_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/holders_dim/holders_dim.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='holders_dim',
                           columns_list=['holder','holder_type']
                          )

    print('tmp_tickers_holders_fact')
    print(tmp_tickers_holders_fact.show(5))
    
    #output path
    tickers_holders_fact_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'tickers_holders_fact/')
    write_files = os.path.join(tickers_holders_fact_path,'tickers_holders_fact.parquet')
    read_files = os.path.join(tickers_holders_fact_path,'tickers_holders_fact_tmp.parquet')
    
    print('update tickers_holders_fact in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_tickers_holders_fact,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/tickers_holders_fact/tickers_holders_fact.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='tickers_holders_fact',
                           columns_list=['ticker','holder']
                          )

if __name__ == "__main__":
    main()