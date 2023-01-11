
import pandas as pd
import os
import boto3
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , DoubleType ,\
                                StringType , IntegerType , DateType ,\
                                TimestampType,DecimalType,LongType

from pyspark.sql.functions import col
from pyspark.sql.functions import concat,concat_ws,col,lit


import configparser
from pyspark.sql import Row

import json
import time

from common_modules import create_spark_session
from common_modules import aws_client
from common_modules import refresh_parquet_files
    
def create_tmp_dimensional_tables_exchanges(dl_bucket_input_prefix, spark):
    """
    This function creates a dataframe containing data from tickers exchanges fetched from input folder
    :param dl_bucket_input_prefix: prefix of the input folder
    :param spark: spark object
    :return: dataframe tmp_exchange_dim
    """
    #input path
    markets_static_data_path = os.path.join(dl_bucket_input_prefix,'markets_static_data/')
    tickers_exchanges_path = os.path.join(markets_static_data_path,'tickers_exchanges/')
    tickers_exchanges_parquet_path = os.path.join(tickers_exchanges_path,'*.parquet')
    
    #input df
    tickers_exchanges_sp_df = spark.read.parquet(tickers_exchanges_parquet_path) 
    
    # exchange dim
    # exchange is PK
    tmp_exchange_dim = tickers_exchanges_sp_df.select('exchange','market').\
        where(col('exchange').isNotNull()).dropDuplicates()

    return tmp_exchange_dim



    
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

    print("create spark session")   
    spark = create_spark_session()

    print('create s3 client')
    s3_client = aws_client(s3_service = 's3', region = region, access_key_id = aws_access_key_id,\
                          secret_access_key = aws_secret_access_key)

    print('create dimensional tables from imported data')
    tmp_exchange_dim = create_tmp_dimensional_tables_exchanges(dl_bucket_input_prefix = DL_BUCKET_INPUT_PREFIX,\
                                          spark = spark)

    print('tmp_exchange_dim')
    print(tmp_exchange_dim.show(5))
    
    #output path
    exchanges_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'exchanges_dim/')
    write_files = os.path.join(exchanges_dim_path,'exchanges_dim.parquet')
    read_files = os.path.join(exchanges_dim_path,'exchanges_dim_tmp.parquet')
    
    print('update exchanges_dim in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_exchange_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/exchanges_dim/exchanges_dim.parquet/',
                           # dl_bucket_output_prefix_0 = DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='exchanges_dim',
                           columns_list=['exchange']
                          )


if __name__ == "__main__":
    main()