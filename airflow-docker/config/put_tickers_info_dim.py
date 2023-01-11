
import pandas as pd
import os
import sys
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

def create_tmp_dimensional_tables_info(dl_bucket_input_prefix, spark):
    """
    This function creates 3 dataframes (tmp_location_dim, tmp_sector_dim, tmp_tickers_info_dim)
    from tickers info related data fetched from input folder.
    :param dl_bucket_input_prefix: prefix of the input folder
    :param spark: spark object
    :return: tmp_location_dim, tmp_sector_dim, tmp_tickers_info_dim
    """
    #input path
    markets_static_data_path = os.path.join(dl_bucket_input_prefix,'markets_static_data/')
    tickers_info_path = os.path.join(markets_static_data_path,'tickers_info/')
    tickers_info_parquet_path = os.path.join(tickers_info_path,'tickers_info.parquet')
    
    #input df
    tickers_info_sp_df = spark.read.parquet(tickers_info_parquet_path) 
    
    # zip is PK
    tmp_location_dim = tickers_info_sp_df.select('city','state','country','zip').\
        where(col('zip').isNotNull()).\
        dropDuplicates()
    
    #industry is PK
    tmp_sector_dim = tickers_info_sp_df.select('industry','sector').\
        where(col('industry').isNotNull()).\
        dropDuplicates()

    #symbol is PK
    #zip is FK
    #industry is FK
    #exhange is FK
    tmp_tickers_info_dim = tickers_info_sp_df.select(col('symbol').alias('ticker'),'industry','zip','currency',
                                                'exchange','fullTimeEmployees',
                                                'fundFamily','fundInceptionDate',
                                                # 'heldPercentInsiders','heldPercentInstitutions',
                                                'isEsgPopulated', 'lastFiscalYearEnd','lastSplitDate',
                                                'lastSplitFactor','longBusinessSummary','longName',
                                                'marketCap','mostRecentQuarter','nextFiscalYearEnd',
                                                'numberOfAnalystOpinions','quoteType','shortName','startDate','uuid'
                                                ).where(col('symbol').isNotNull()).\
                                                dropDuplicates()
    return tmp_location_dim, tmp_sector_dim, tmp_tickers_info_dim

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
    tmp_location_dim, tmp_sector_dim, tmp_tickers_info_dim =\
        create_tmp_dimensional_tables_info(dl_bucket_input_prefix = DL_BUCKET_INPUT_PREFIX,\
                                          spark = spark)

    print('tmp_location_dim')
    print(tmp_location_dim.show(5))

    print('tmp_sector_dim')
    print(tmp_sector_dim.show(5))
    
    print('tmp_tickers_info_dim')
    print(tmp_tickers_info_dim.show(5))

    print('update location_dim in datalake')
    #output path
    location_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'location_dim/')
    write_files = os.path.join(location_dim_path,'location_dim.parquet')
    read_files = os.path.join(location_dim_path,'location_dim_tmp.parquet')
    
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_location_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/location_dim/location_dim.parquet/',
                           columns_list=['zip']
                          )

    print('update sector_dim in datalake')
    #output path
    sector_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'sector_dim/')
    write_files = os.path.join(sector_dim_path,'sector_dim.parquet')
    read_files = os.path.join(sector_dim_path,'sector_dim_tmp.parquet')
    
    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_sector_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/sector_dim/sector_dim.parquet/',
                           columns_list=['industry']
                          )
    
    print('update tmp_tickers_info_dim with isin')    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_isin_path = os.path.join(markets_static_data_path,'tickers_isin/')
    tickers_isin_parquet_path = os.path.join(tickers_isin_path,'tickers_isin.parquet')
    tickers_isin_sp_df = spark.read.parquet(tickers_isin_parquet_path) 
    tickers_isin_sp_df = tickers_isin_sp_df.select('isin',col('symbol').alias('ticker'))

    print('tickers_isin df')
    print(tickers_isin_sp_df.show(5))
        
    tmp_tickers_info_dim = tmp_tickers_info_dim.join(tickers_isin_sp_df,\
         tmp_tickers_info_dim['ticker'] == tickers_isin_sp_df['ticker'] , 'left').\
         select(tmp_tickers_info_dim.ticker,'industry','zip','currency',tickers_isin_sp_df.isin,
                                            'exchange','fullTimeEmployees',
                                            'fundFamily','fundInceptionDate',
                                            # 'heldPercentInsiders','heldPercentInstitutions',
                                            'isEsgPopulated', 'lastFiscalYearEnd','lastSplitDate',
                                            'lastSplitFactor','longBusinessSummary','longName',
                                            'marketCap','mostRecentQuarter','nextFiscalYearEnd',
                                            'numberOfAnalystOpinions','quoteType','shortName',
                                            'startDate','uuid'
                                            )
    print('tmp_tickers_info_dim with isin')
    print(tmp_tickers_info_dim.show(5))
    
    
    print('update tmp_tickers_info_dim with major_holders')      
    tickers_major_holders_path = os.path.join(markets_static_data_path,'tickers_major_holders/')
    tickers_major_holders_parquet_path = os.path.join(tickers_major_holders_path,'tickers_major_holders.parquet')
    tickers_major_holders_sp_df = spark.read.parquet(tickers_major_holders_parquet_path) 
    tickers_major_holders_sp_df = tickers_major_holders_sp_df.withColumn('ticker',col('symbol')).\
        drop(tickers_major_holders_sp_df.symbol)
    
    print('tickers_major_holders df')
    print(tickers_major_holders_sp_df.show(5))
    tmp_tickers_info_dim = tmp_tickers_info_dim.join(tickers_major_holders_sp_df,\
         tmp_tickers_info_dim['ticker'] == tickers_major_holders_sp_df['ticker'] , 'left').\
         select(tmp_tickers_info_dim.ticker,'industry','zip','currency','isin',
                'exchange','fullTimeEmployees',
                'fundFamily','fundInceptionDate',
                # 'heldPercentInsiders','heldPercentInstitutions',
                'isEsgPopulated', 'lastFiscalYearEnd','lastSplitDate',
                'lastSplitFactor','longBusinessSummary','longName',
                'marketCap','mostRecentQuarter','nextFiscalYearEnd',
                'numberOfAnalystOpinions','quoteType','shortName',
                'startDate','uuid',
                tickers_major_holders_sp_df.percentage_of_shares_held_by_all_insider,\
        tickers_major_holders_sp_df.percentage_of_shares_held_by_institutions,\
        tickers_major_holders_sp_df.percentage_of_float_held_by_institutions,\
        tickers_major_holders_sp_df.number_of_institutions_holding_shares)

    print('tmp_tickers_info_dim with major holders')
    print(tmp_tickers_info_dim.show(5))

    print('remove duplicates from tmp_tickers_info_dim')
    tmp_tickers_info_dim = tmp_tickers_info_dim.distinct()

    print('update tickers_info_dim in datalake')
    #output path
    tickers_info_dim_path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,'tickers_info_dim/')
    write_files = os.path.join(tickers_info_dim_path,'tickers_info_dim.parquet')
    read_files = os.path.join(tickers_info_dim_path,'tickers_info_dim_tmp.parquet')
    

    refresh_parquet_files (write_files = write_files,\
                           read_files = read_files,\
                           tmp_df = tmp_tickers_info_dim,\
                           spark = spark,\
                           s3_client = s3_client,\
                           dl_bucket_name = DL_BUCKET_NAME,
                           dl_bucket_output_prefix = 'datalake/tickers_info_dim/tickers_info_dim.parquet/',
                           # dl_bucket_output_prefix_0=DL_BUCKET_OUTPUT_PREFIX,
                           # dim_table_name='tickers_info_dim',
                           columns_list=['ticker']
                          )
    
if __name__ == "__main__":
    main()