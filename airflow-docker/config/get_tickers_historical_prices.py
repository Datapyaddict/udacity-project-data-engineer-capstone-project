
import pandas as pd
import os
import sys
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


def get_tickers_historical_prices(multiple_tickers, interval, period):
    """
    Based on the list of tickers , the function returns a list of tickers historical prices based on
    the intervals and the max period.
    If interval = 1m ( one minute), max period = 7 days.
    If interval = 1h ( one minute), max period = 2 years.
    If interval = 1d ( one minute), max period = 10 years.
    :param multiple_tickers: string with multiple tickers separated by a blank space
    :param interval: interval parameter
    :param period: period parameter
    :return: historical prices object
    """

    historical_prices = yf.download(multiple_tickers, interval = interval,period= period,group_by='ticker',\
                                    threads=True,progress=True,\
                      show_errors=True,  prepost=True,auto_adjust = True)
    
    historical_prices = historical_prices.stack(level=0).reset_index()
    historical_prices = historical_prices.rename(columns = {'level_1' : 'Ticker'})
    historical_prices['Interval'] = interval
    if interval == '1m':
        historical_prices = historical_prices.rename(columns = {'level_1' : 'Ticker', 'level_0' :'Datetime'})
        historical_prices['Date'] = historical_prices['Datetime'].dt.strftime('%Y-%m-%d')
        historical_prices['Datetime'] = historical_prices['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S' )
    elif interval == '1d':
        historical_prices = historical_prices.rename(columns = {'level_1' : 'Ticker'})
        historical_prices['Datetime'] = historical_prices['Date'].dt.strftime('%Y-%m-%d %H:%M:%S' )
        historical_prices['Date'] = historical_prices['Date'].dt.strftime('%Y-%m-%d')
    elif interval == '1h':
        historical_prices = historical_prices.rename(columns = {'level_1' : 'Ticker', 'level_0' : 'Datetime'})
        historical_prices['Date'] = historical_prices['Datetime'].dt.strftime('%Y-%m-%d')
        historical_prices['Datetime'] = historical_prices['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S' )
        
    historical_prices = historical_prices[['Datetime','Date','Ticker','Close','High','Low','Open','Volume','Interval']]
            
    return historical_prices

def write_tickers_historical_prices_to_parquet(historical_prices,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers historical prices object and writes a parquet file.
    :param historical_prices: historical prices object
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """

    print('create dataframe of tickers historical prices')
    
    historical_prices_sp_df=spark.createDataFrame(data = historical_prices)
                
    print('write tickers_balancesheet.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_historical_prices_path = os.path.join(markets_static_data_path,'tickers_historical_prices/')

    dump_file = os.path.join(tickers_historical_prices_path,'tickers_historical_prices.parquet')

    historical_prices_sp_df.write.mode('overwrite').parquet(dump_file)
    historical_prices_sp_df.show(truncate = False)
    
    
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
    
    # print('get tickers objects')
    # tickers = get_tickers_object(multiple_tickers)

    print('get tickers historical prices')    
    historical_prices_1m = get_tickers_historical_prices(multiple_tickers, interval = '1m', period = '7d')
    historical_prices_1d = get_tickers_historical_prices(multiple_tickers, interval = '1d', period = '10y')
    historical_prices_1h = get_tickers_historical_prices(multiple_tickers, interval = '1h', period = '2y')
    
    historical_prices = pd.concat([historical_prices_1d,historical_prices_1m,historical_prices_1h], axis = 0)
        
    print('write tickers historical prices parquet file') 
    write_tickers_historical_prices_to_parquet(historical_prices, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()