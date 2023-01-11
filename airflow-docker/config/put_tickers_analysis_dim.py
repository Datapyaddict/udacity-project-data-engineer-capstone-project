
# import pandas as pd
import os
import sys
# import boto3
# import shutil
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType , StructField , DoubleType ,\
#                                 StringType , IntegerType , DateType ,\
#                                 TimestampType,DecimalType,LongType


# from pyspark import SparkConf,SparkContext
# from pyspark.sql import functions as F
# from pyspark.sql.functions import udf
# from pyspark.sql.functions import concat,concat_ws,col,lit
# from pyspark.sql.functions import col, year, month, dayofweek, hour, weekofyear, dayofmonth,monotonically_increasing_id
from pyspark.sql.functions import expr, col
# from pyspark.sql.functions import when
# from pyspark.sql.functions import max
# from pyspark.sql.window import Window

import configparser
# from pyspark.sql import Row
#
# import json
# import time

from common_modules import create_spark_session
from common_modules import aws_client
from common_modules import refresh_parquet_files

def create_tmp_dimensional_tables_analysis(dl_bucket_input_prefix, spark):
    """
    This function creates a dataframe (tmp_tickers_analysis_fact) from tickers
    market analysis related data fetched from input folder
    :param dl_bucket_input_prefix: prefix of the input folder
    :param spark: spark object
    :return: tmp_tickers_analysis_fact
    """
    #input path
    markets_static_data_path = os.path.join(dl_bucket_input_prefix,'markets_static_data/')
    tickers_analysis_path = os.path.join(markets_static_data_path,'tickers_analysis/')
    tickers_analysis_parquet_path = os.path.join(tickers_analysis_path,'tickers_analysis.parquet')
    
    #input df
    tickers_analysis_sp_df = spark.read.parquet(tickers_analysis_parquet_path) 
           
    #tmp_tickers_analysis_fact
    tmp_tickers_analysis_fact = tickers_analysis_sp_df.\
        select(col('symbol').alias('ticker'),'period','growth','earnings_estimate_avg' ,
               'earnings_estimate_low' ,
    'earnings_estimate_high' ,'earnings_estimate_year_ago_eps' ,
    'earnings_estimate_number_of_analysts' ,'earnings_estimate_growth','revenue_estimate_avg' ,
    'revenue_estimate_low','revenue_estimate_high' ,'eps_trend_current' ,'eps_trend_7days_ago' ,
    'eps_trend_30days_ago' ,'eps_trend_60days_ago' ,'eps_trend_90days_ago' ,'eps_revisions_up_last7days' ,
    'eps_revisions_up_last30days','eps_revisions_down_last30days' ,'eps_revisions_down_last90days')

    return tmp_tickers_analysis_fact

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
    tmp_tickers_analysis_fact =\
        create_tmp_dimensional_tables_analysis(dl_bucket_input_prefix = DL_BUCKET_INPUT_PREFIX,\
                                          spark = spark)

    print('tmp_tickers_analysis_fact')
    print(tmp_tickers_analysis_fact.show(5))
    
    #output path
    tickers_analysis_fact_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'tickers_analysis_fact/')
    write_files = os.path.join(tickers_analysis_fact_path,'tickers_analysis_fact.parquet')
    read_files = os.path.join(tickers_analysis_fact_path,'tickers_analysis_fact_tmp.parquet')
    
    print('update tickers_analysis_fact in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_tickers_analysis_fact,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/tickers_analysis_fact/tickers_analysis_fact.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name = 'tickers_analysis_fact',
                           columns_list = ['ticker','period']
                          )

if __name__ == "__main__":
    main()
