
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

def get_tickers_holders(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers holders info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers holders.
    """
    tickers_holders_ls = []
    print('load institutional holders')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            institutional_holders = yf.Ticker(f"{tickers_ls[i]}").institutional_holders
            institutional_holders['Date Reported'] = institutional_holders['Date Reported'].astype('str')
            institutional_holders = institutional_holders.to_dict()
            institutional_holders['symbol'] = tickers_ls[i]
            institutional_holders['holder_type'] = 'institutional'
            tickers_holders_ls.append(institutional_holders)

        except Exception as e:
            print(e)
            continue
            
    print('load mutual fund holders')        
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            mutualfund_holders = yf.Ticker(f"{tickers_ls[i]}").mutualfund_holders
            mutualfund_holders['Date Reported'] = mutualfund_holders['Date Reported'].dt.strftime('%Y-%m-%d')#astype('str')
            mutualfund_holders = mutualfund_holders.to_dict()
            mutualfund_holders['symbol'] = tickers_ls[i]
            mutualfund_holders['holder_type'] = 'mutual fund'
            tickers_holders_ls.append(mutualfund_holders)

        except Exception as e:
            print(e)
            continue
            
    return tickers_holders_ls

def write_tickers_holders_to_parquet(tickers_holders_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers holders info and writes the data into a parquet file
    in input folder.
    :param tickers_holders_ls: tickers holders list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_holders = StructType([ \
    StructField('Holder',StringType(),True),\
    StructField('Shares',IntegerType(),True),\
    StructField('Date Reported',StringType(),True),\
    StructField('% Out',DoubleType(),True),\
    StructField('Value',LongType(),True),\
    StructField('symbol',StringType(),True),\
    StructField('holder_type',StringType(),True)
      ])

    print('create dataframe of holders for tickers')
    
    target_columns = ['Holder','Shares',\
                                'Date Reported', '% Out','Value',\
                               'symbol','holder_type']
    
    df = pd.DataFrame([],columns = target_columns)
    
    for symbol in tickers_holders_ls:
        symbol_df = pd.DataFrame(symbol)
        columns = [x for x in symbol.keys()]
        if len(symbol_df) ==0 :
            print(symbol)
            continue

        else :
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column)  
            
    print('write tickers_holders.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_holders_path = os.path.join(markets_static_data_path,'tickers_holders/')

    dump_file = os.path.join(tickers_holders_path,'tickers_holders.parquet')
    
    df['Shares'] = df['Shares'].astype('int64')
    df['% Out'] = df['% Out'].astype('float64')
    df['Value'] = df['Value'].astype('int64')

    tickers_holders_sp_df = spark.createDataFrame(df, schema = schema_ticker_holders)

    tickers_holders_sp_df = tickers_holders_sp_df.\
    withColumn( 'holder'  ,col('Holder')).\
    withColumn( 'shares' ,col('Shares').cast(DecimalType(20, 2))).\
    withColumn( 'date_reported'  ,col( 'Date Reported')).\
    withColumn( 'out'  ,col( '% Out').cast(DecimalType(20, 2))).\
    withColumn( 'value'  ,col( 'Value'))

    tickers_holders_sp_df = tickers_holders_sp_df.\
            select('symbol','holder','holder_type','shares','date_reported','out', 'value' )

    tickers_holders_sp_df.write.mode('overwrite').parquet(dump_file)
    tickers_holders_sp_df.show(truncate = False)
    
    
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

    print('get ticker holders')    
    tickers_holders_ls = get_tickers_holders(tickers_ls,tickers)
    
    print('write tickers holders info parquet file') 
    write_tickers_holders_to_parquet(tickers_holders_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()