
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

import yfinance as yf

def get_tickers_actions(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers actions info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers actions.
    """

    tickers_actions_ls = []

    print('load yearly actions')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            actions = tickers.tickers[f"{str(tickers_ls[i])}"].actions
            actions = actions.to_dict()
            actions['symbol'] = tickers_ls[i]
            tickers_actions_ls.append(actions)

        except Exception as e:
            print(e)
            continue

    return tickers_actions_ls

def write_tickers_actions_to_parquet(tickers_actions_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers actions info and writes the data into a parquet file
    in input folder.
    :param tickers_actions_ls: tickers actions list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """

    schema_tickers_actions = StructType([ \
    # StructField('Date',DateType(),True),
    StructField('Date',TimestampType(),True),
    StructField('Dividends',DoubleType(),True), 
    StructField('Stock Splits',DoubleType(),True),
    StructField('symbol',StringType(),True)
      ])

    print('create dataframe of actions for tickers')
    
    target_columns = ['Date','Dividends','Stock Splits', 'symbol']
    
    df = pd.DataFrame([],columns = target_columns)
    
    for symbol in tickers_actions_ls:
        try :
            symbol_df = pd.DataFrame(symbol).reset_index().rename(columns = {'index':'Date'})
        except Exception: 
            print(symbol)
            continue
        else:    
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column)  
            
    print('write tickers_actions.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_actions_path = os.path.join(markets_static_data_path,'tickers_actions/')

    dump_file = os.path.join(tickers_actions_path,'tickers_actions.parquet')
    
    tickers_actions_sp_df = spark.createDataFrame(df, schema = schema_tickers_actions)

    tickers_actions_sp_df = tickers_actions_sp_df.\
    withColumn( 'dividends'  ,col('Dividends').cast(DecimalType(20, 2))).\
    withColumn( 'stock_splits' ,col('Stock Splits').cast(DecimalType(20, 2))).\
    withColumn('date', col('Date'))


    tickers_actions_sp_df = tickers_actions_sp_df.select('date','symbol','dividends','stock_splits')

    tickers_actions_sp_df.write.mode('overwrite').parquet(dump_file)
    tickers_actions_sp_df.show(truncate = False)
    
    
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

    print('get tickers actions')    
    tickers_actions_ls = get_tickers_actions(tickers_ls,tickers)
    
    print('write tickers actions info parquet file') 
    write_tickers_actions_to_parquet(tickers_actions_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()