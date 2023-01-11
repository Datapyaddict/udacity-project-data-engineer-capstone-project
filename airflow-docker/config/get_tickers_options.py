
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


def get_tickers_options(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers options info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers options.
    """
    tickers_options_ls = []
    print('load tickers options')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            calls = yf.Ticker(f"{tickers_ls[i]}").option_chain().calls
            calls['lastTradeDate'] = calls['lastTradeDate'].astype('str')
            calls['option_type'] = 'call'  
            
            puts = yf.Ticker(f"{tickers_ls[i]}").option_chain().puts
            puts['lastTradeDate'] = puts['lastTradeDate'].astype('str')    
            puts['option_type'] = 'put'
            
            options = pd.concat([calls, puts], axis = 0) 
            options = options.to_dict()
            options['symbol'] = tickers_ls[i]
            tickers_options_ls.append(options)
        except (Exception,TypeError) as e:
            print('error on call option :',e)
      
          
    return tickers_options_ls

def write_tickers_options_to_parquet(tickers_options_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers options and writes the data into a parquet file
    in input folder.
    :param tickers_options_ls: tickers options list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_options = StructType([\
    StructField('contractSymbol',StringType(),True),\
    StructField('lastTradeDate',StringType(),True),\
    StructField('strike',StringType(),True),\
    StructField('lastPrice',StringType(),True),\
    StructField('bid',StringType(),True),\
    StructField('ask',StringType(),True),\
    StructField('change',StringType(),True),\
    StructField('percentChange',StringType(),True),\
    StructField('volume',StringType(),True),\
    StructField('openInterest',StringType()),\
    StructField('impliedVolatility',StringType(),True),\
    StructField('inTheMoney',StringType(),True),\
    StructField('contractSize',StringType(),True),\
    StructField('currency',StringType(),True),\
    StructField('option_type',StringType(),True),\
    StructField('symbol',StringType(),True)
                ])

    print('create dataframe of tickers options')
    
    target_columns = ['contractSymbol', 'lastTradeDate', 'strike', 'lastPrice', 'bid', 'ask',
       'change', 'percentChange', 'volume', 'openInterest',
       'impliedVolatility', 'inTheMoney', 'contractSize', 'currency',
       'option_type', 'symbol']
    
    df = pd.DataFrame([],columns = target_columns)

    for symbol in tickers_options_ls:
        symbol_df = pd.DataFrame(symbol)
        if len(symbol_df) ==0 :
            print(symbol)
            continue

        else :
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column)        
            
    print('write tickers_options.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_options_path = os.path.join(markets_static_data_path,'tickers_options/')

    dump_file = os.path.join(tickers_options_path,'tickers_options.parquet')
    print(df.columns)
    print(df.shape)
    
    ticker_options_sp_df = spark.createDataFrame(df, schema = schema_ticker_options)

    ticker_options_sp_df = ticker_options_sp_df.\
        withColumn( 'contract_symbol'  ,col('contractSymbol')).\
        withColumn( 'last_trade_date'  ,col('lastTradeDate')).\
        withColumn( 'last_price'  ,col('lastPrice')).\
        withColumn( 'percent_change'  ,col('percentChange')).\
        withColumn( 'open_interest'  ,col('openInterest')).\
        withColumn( 'implied_volatility'  ,col('impliedVolatility')).\
        withColumn( 'in_the_money'  ,col('inTheMoney')).\
        withColumn( 'contract_size'  ,col('contractSize'))





    ticker_options_sp_df = ticker_options_sp_df.\
            select('contract_symbol', 'last_trade_date', 'strike', 'last_price', 'bid', 'ask',
           'change', 'percent_change', 'volume', 'open_interest',
           'implied_volatility', 'in_the_money', 'contract_size', 'currency',
           'option_type', 'symbol')
    ticker_options_sp_df.write.mode('overwrite').parquet(dump_file)
    ticker_options_sp_df.show(truncate = False)
    
    
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

    print('get tickers options')    
    tickers_options_ls = get_tickers_options(tickers_ls,tickers)
    
    print('write tickers coptionsinfo parquet file') 
    write_tickers_options_to_parquet(tickers_options_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()
