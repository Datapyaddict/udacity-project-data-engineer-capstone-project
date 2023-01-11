
import pandas as pd
import os
import sys
import yfinance as yf

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , DoubleType ,\
                                StringType , IntegerType , DateType ,\
                                TimestampType,DecimalType,LongType

from pyspark.sql.functions import col


import configparser
import yfinance as yf  

from common_modules import create_spark_session
from common_modules import get_tickers_ls
from common_modules import get_tickers_object


def get_tickers_shares(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers shares info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers shares.
    """
    tickers_shares_ls = []
    print('load tickers shares')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            shares = yf.Ticker(f"{tickers_ls[i]}").shares
            shares = shares.reset_index()
            shares = shares.to_dict()
            shares['symbol'] = tickers_ls[i]
            tickers_shares_ls.append(shares)

        except Exception as e:
            print(e)
            continue
            
    return tickers_shares_ls

def write_tickers_shares_to_parquet(tickers_shares_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers shares and writes the data into a parquet file
    in input folder.
    :param tickers_shares_ls: tickers shares list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_shares = StructType([\
    StructField('Year',StringType(),True),\
    StructField('BasicShares',LongType(),True),\
    StructField('symbol',StringType(),True)
                ])

    print('create dataframe of tickers shares')
    
    target_columns = ['Year', 'BasicShares', 'symbol']
    
    df = pd.DataFrame([],columns = target_columns)

    for symbol in tickers_shares_ls:
        symbol_df = pd.DataFrame(symbol)[['Year', 'BasicShares','symbol']]
        if len(symbol_df) ==0 :
            print(symbol)
            continue

        else :
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column)        
            
    print('write tickers_shares.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_shares_path = os.path.join(markets_static_data_path,'tickers_shares/')

    dump_file = os.path.join(tickers_shares_path,'tickers_shares.parquet')
    print(df.columns)
    print(df.shape)
    
    ticker_shares_sp_df = spark.createDataFrame(df, schema = schema_ticker_shares)

    ticker_shares_sp_df = ticker_shares_sp_df.\
        withColumn( 'year'  ,col('Year')).\
        withColumn( 'basic_shares'  ,col('BasicShares'))

    ticker_shares_sp_df = ticker_shares_sp_df.\
            select('year', 'basic_shares','symbol')
    ticker_shares_sp_df.write.mode('overwrite').parquet(dump_file)
    ticker_shares_sp_df.show(truncate = False)
    
    
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

    print('get tickers shares')    
    tickers_shares_ls = get_tickers_shares(tickers_ls,tickers)
    
    print('write tickers shares info parquet file') 
    write_tickers_shares_to_parquet(tickers_shares_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()
