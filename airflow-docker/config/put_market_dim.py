
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

import sys

import json
import time

from common_modules import create_spark_session
from common_modules import aws_client
from common_modules import refresh_parquet_files


    
def create_tmp_dimensional_tables_market_indexes(dl_bucket_input_prefix, spark):
    """
    This function creates 2 dataframe (tmp_market_indexes_dim, tmp_market_dim) from tickers
    market indexes fetched from input folder
    :param dl_bucket_input_prefix: prefix of the input folder
    :param spark: spark object
    :return: tmp_market_indexes_dim, tmp_market_dim
    """
    #input path
    markets_static_data_path = os.path.join(dl_bucket_input_prefix,'markets_static_data/')
    market_indexes_path = os.path.join(markets_static_data_path,'market_indexes/')
    market_indexes_parquet_path = os.path.join(market_indexes_path,'markets_indexes.parquet')
    
    #input df
    market_indexes_sp_df = spark.read.parquet(market_indexes_parquet_path) 
    
    indexes = market_indexes_sp_df.select('index','abbreviation','market').\
        withColumn('country',col('market')).\
        withColumn('suffix', lit('_market')).\
        select(col('index'),concat(col('abbreviation'),col('suffix')).alias('market'),col('country')).toPandas()
    
    rows_ls = []

    for row in indexes.values:
        market = row[1]
        country = row[2]
        for index in row[0]:
            row = Row(market = market,country = country, index_id = index.id, index_name = index.name )
            rows_ls.append(row)
        
    tmp_market_indexes_dim = spark.createDataFrame(rows_ls).select('index_id','index_name','market').\
        where(col('index_id').isNotNull()).\
        sort(col('market').asc())
    
    tmp_market_dim = spark.createDataFrame(rows_ls).select('market', 'country').\
        where(col('index_id').isNotNull()).distinct().\
        sort(col('market').asc())

    return tmp_market_indexes_dim, tmp_market_dim

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
    tmp_market_indexes_dim, tmp_market_dim = create_tmp_dimensional_tables_market_indexes(dl_bucket_input_prefix = DL_BUCKET_INPUT_PREFIX,\
                                          spark = spark)

    print('tmp_market_indexes_dim')
    print(tmp_market_indexes_dim.show(5))
    
    #output path
    market_indexes_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'market_indexes_dim/')
    write_files = os.path.join(market_indexes_dim_path,'market_indexes_dim.parquet')
    read_files = os.path.join(market_indexes_dim_path,'market_indexes_dim_tmp.parquet')
    
    print('update market_indexes_dim in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_market_indexes_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/market_indexes_dim/market_indexes.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='market_indexes_dim',
                           columns_list=['index_id','market']
                          )

   
    
    print('tmp_market_dim')
    print(tmp_market_dim.show(5))
    
    #output path
    market_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'markets_dim/')
    write_files = os.path.join(market_dim_path,'markets_dim.parquet')
    read_files = os.path.join(market_dim_path,'markets_dim_tmp.parquet')
    
    print('update market_dim in datalake')
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_market_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/markets_dim/markets_dim.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name = 'markets_dim',
                           columns_list = ['market']
                          )

if __name__ == "__main__":
    main()