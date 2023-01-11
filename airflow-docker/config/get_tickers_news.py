
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

def get_tickers_news(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers news info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers news.
    """
    tickers_news_ls = []
    print('load tickers isin')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            news_ls = yf.Ticker(f"{tickers_ls[i]}").news
        except Exception as e:
            print(e)
            continue
        if len(news_ls) != 0 :
            for news in news_ls:
                try:
                    news['symbol'] = tickers_ls[i]
                    tickers_news_ls.append(news_ls)
                except:
                    continue
    return tickers_news_ls

def write_tickers_news_to_parquet(tickers_news_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers news and writes the data into a parquet file
    in input folder.
    :param tickers_news_ls: tickers news list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_news = StructType([\
    StructField('uuid',StringType(),True),\
    StructField('title',StringType(),True),\
    StructField('publisher',StringType(),True),\
    StructField('link',StringType(),True),\
    StructField('providerPublishTime',StringType(),True),\
    StructField('type',StringType(),True),\
    StructField('symbol',StringType(),True)
                ])

    print('create dataframe of tickers news')
    
    target_columns =  ['uuid', 'title', 'publisher', 'link', 'providerPublishTime', 'type',
       'symbol']
    df = pd.DataFrame([],columns = target_columns)
    
    for symbol in tickers_news_ls:
        symbol_df = pd.DataFrame(symbol)[['uuid', 'title', 'publisher', 'link', 'providerPublishTime', 'type',
          'symbol']]
        if len(symbol_df) ==0 :
            print('no news item for ',symbol)
            continue
        
        else :
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)
            
    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column) 
            
    print('write tickers_news.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_news_path = os.path.join(markets_static_data_path,'tickers_news/')

    dump_file = os.path.join(tickers_news_path,'tickers_news.parquet')
    print(df.columns)
    print(df.shape)
    
    ticker_news_sp_df = spark.createDataFrame(df, schema = schema_ticker_news)

    ticker_news_sp_df = ticker_news_sp_df.\
    withColumn( 'provider_publish_time'  ,col('providerPublishTime'))

    ticker_news_sp_df = ticker_news_sp_df.\
            select('uuid', 'title', 'publisher', 'link', 'provider_publish_time', 'type',
           'symbol')

    ticker_news_sp_df.write.mode('overwrite').parquet(dump_file)
    ticker_news_sp_df.show(truncate = False)
    
    
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

    print('get tickers news')    
    tickers_news_ls = get_tickers_news(tickers_ls,tickers)
    
    print('write tickers isin info parquet file') 
    write_tickers_news_to_parquet(tickers_news_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()
