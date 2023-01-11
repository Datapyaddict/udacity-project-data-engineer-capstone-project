
import pandas as pd
import os
import sys
# from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
# from pyspark.sql import SQLContext
from pyspark.sql.types import StructType , StructField , DoubleType ,\
                                StringType , IntegerType , DateType ,\
                                TimestampType,DecimalType,LongType
# import time
# import json
import configparser
import yfinance as yf  

from common_modules import create_spark_session
from common_modules import get_tickers_ls
from common_modules import get_tickers_object

def get_tickers_info(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers info.
    """
    ticker_info_ls = []
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            info = tickers.tickers[f"{tickers_ls[i]}"].info
            ticker_info_ls.append(info)
        except Exception as e:
            print(e)
            continue
    return ticker_info_ls

def write_tickers_info_to_parquet(ticker_info_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers info and writes the data into a parquet file
    in input folder.
    :param tickers_info_ls: tickers info list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_general_info = StructType([ \
    StructField("symbol",StringType(),True),
    StructField("address1",StringType(),True),                   
    StructField("address2",StringType(),True),
    StructField("category",StringType(),True),
    StructField("city",StringType(),True),
    StructField("country",StringType(),True),
    StructField("currency",StringType(),True),
    StructField("exchange",StringType(),True),
    StructField("exchangeTimezoneShortName",StringType(),True),
    StructField("fax",StringType(),True),
    StructField("fullTimeEmployees",StringType(),True),
    StructField("fundFamily",StringType(),True),
    StructField("fundInceptionDate",StringType(),True),
    StructField("heldPercentInsiders",StringType(),True),
    StructField("heldPercentInstitutions",StringType(),True),
    StructField("industry",StringType(),True),
    StructField("isEsgPopulated",StringType(),True),
    StructField("lastFiscalYearEnd",StringType(),True),
    StructField("lastSplitDate",StringType(),True),
    StructField("lastSplitFactor",StringType(),True),
    StructField("legalType",StringType(),True),
    StructField("logo_url",StringType(),True),
    StructField("longBusinessSummary",StringType(),True),
    StructField("longName",StringType(),True),
    StructField("market",StringType(),True),
    StructField("marketCap",StringType(),True),
    StructField("marketState",StringType(),True),
    StructField("mostRecentQuarter",StringType(),True),
    StructField("nextFiscalYearEnd",StringType(),True),
    StructField("numberOfAnalystOpinions",StringType(),True),
    StructField("phone",StringType(),True),
    StructField("quoteSourceName",StringType(),True),
    StructField("quoteType",StringType(),True),
    StructField("regularMarketSource",StringType(),True),
    StructField("regularMarketTime",StringType(),True),
    StructField("sector",StringType(),True),
    StructField("shortName",StringType(),True),
    StructField("startDate",StringType(),True),
    StructField("state",StringType(),True),
    StructField("tradeable",StringType(),True),
    StructField("uuid",StringType(),True),
    StructField("website",StringType(),True),
    StructField("zip",StringType(),True)                                   
      ])


    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_info_path = os.path.join(markets_static_data_path,'tickers_info/')

    dump_file = os.path.join(tickers_info_path,f'tickers_info.parquet')
    tickers_info_sp_df = spark.createDataFrame(ticker_info_ls, schema = schema_ticker_general_info)
    tickers_info_sp_df.show(truncate = False)
    tickers_info_sp_df.write.mode('overwrite').parquet(dump_file)
    
def main():

    config = configparser.ConfigParser()    
    config.read('/home/hadoop/dl_config.cfg')  
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    DL_BUCKET_INPUT_PREFIX = config.get('S3_BUCKET','DL_BUCKET_INPUT_PREFIX')
    DL_BUCKET_NAME = config.get('S3_BUCKET','DL_BUCKET_NAME')
    DL_BUCKET_SCRIPTS_FOLDER = config.get('S3_BUCKET','DL_BUCKET_SCRIPTS_FOLDER')
    num_tickers = int(config.get('APIS','TOP_N_TICKERS'))

    print('python version : ', sys.version)

    print("create spark session")   
    spark = create_spark_session()
    
    constituents_csv_path = os.path.join('s3://',DL_BUCKET_NAME,DL_BUCKET_SCRIPTS_FOLDER,'constituents.csv')
    print(constituents_csv_path)

    print(f"get {num_tickers} tickers list")
    tickers_ls, multiple_tickers = get_tickers_ls(num_tickers,constituents_csv_path,spark)
    
    print('get tickers objects')       
    tickers = get_tickers_object(multiple_tickers)

    print('get tickers info')    
    ticker_info_ls = get_tickers_info(tickers_ls,tickers)
    
    print('write tickers info') 
    write_tickers_info_to_parquet(ticker_info_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()