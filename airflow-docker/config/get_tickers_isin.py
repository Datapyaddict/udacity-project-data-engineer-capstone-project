
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

def get_tickers_isin(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers isin info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers isin.
    """
    tickers_isin_ls = []
    isin_tmp = dict()
    print('load tickers isin')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            isin = yf.Ticker(f"{tickers_ls[i]}").get_isin()
            isin_tmp['isin'] = isin
            isin_tmp['symbol'] = tickers_ls[i]
            tickers_isin_ls.append(isin_tmp)

        except Exception as e:
            print(e)
            continue
                        
    return tickers_isin_ls

def write_tickers_isin_to_parquet(tickers_isin_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers isin info and writes the data into a parquet file
    in input folder.
    :param tickers_isin_ls: tickers isin list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_isin = StructType([\
    StructField('isin',StringType(),True),\
    StructField('symbol',StringType(),True)
                ])

    print('create dataframe of tickers isin')
    
    df = pd.DataFrame(tickers_isin_ls)        
            
    print('write tickers_isin.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_isin_path = os.path.join(markets_static_data_path,'tickers_isin/')

    dump_file = os.path.join(tickers_isin_path,'tickers_isin.parquet')
    print(df.columns)
    print(df.shape)
    
    tickers_isin_sp_df = spark.createDataFrame(df, schema = schema_ticker_isin)


    tickers_isin_sp_df.write.mode('overwrite').parquet(dump_file)
    tickers_isin_sp_df.show(truncate = False)
    
    
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

    print('get tickers isin')    
    tickers_isin_ls = get_tickers_isin(tickers_ls,tickers)
    
    print('write tickers isin info parquet file') 
    write_tickers_isin_to_parquet(tickers_isin_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()