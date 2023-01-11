
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

def get_tickers_earnings_dates(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers earnings_dates info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers earnings_dates.
    """
    tickers_earnings_dates_ls = []
    print('load tickers earnings per share dates')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            earnings_dates = tickers.tickers[f"{tickers_ls[i]}"].earnings_dates
        except Exception as e:
            print(e)
            continue
        else:            
            earnings_dates = earnings_dates.reset_index()
            earnings_dates['Earnings Date'] = earnings_dates['Earnings Date'].dt.strftime('%Y-%m-%d')#astype('str')
            earnings_dates = earnings_dates[['Earnings Date','EPS Estimate','Reported EPS','Surprise(%)']]
            earnings_dates = earnings_dates.to_dict()
            earnings_dates['symbol'] = tickers_ls[i]
            tickers_earnings_dates_ls.append(earnings_dates)

    return tickers_earnings_dates_ls

def write_tickers_earnings_dates_to_parquet(tickers_earnings_dates_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers earnings_dates info and writes the data into a parquet file
    in input folder.
    :param tickers_earnings_dates_ls: tickers earnings_dates list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_tickers_eps = StructType([ \
    StructField('Earnings Date',StringType(),True),
    StructField('EPS Estimate',DoubleType(),True), 
    StructField('Reported EPS',DoubleType(),True),
    StructField('Surprise(%)',DoubleType(),True), 
    StructField('symbol',StringType(),True)
      ])

    print('create dataframe of tickers earnings per share dates')
    
    target_columns = ['Earnings Date','EPS Estimate', 'Reported EPS',\
                                'Surprise(%)', 'symbol']
    
    df = pd.DataFrame([],columns = target_columns)

    for symbol in tickers_earnings_dates_ls:
        try :
            symbol_df = pd.DataFrame(symbol)#.reset_index().rename(columns = {'index':'date'})
        except Exception: 
            print(symbol)
            continue
        else:    
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column)        
            
    print('write tickers_eps.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_eps_path = os.path.join(markets_static_data_path,'tickers_eps/')

    dump_file = os.path.join(tickers_eps_path,'tickers_eps.parquet')
    print(df.columns)
    print(df.shape)
    
    tickers_eps_sp_df = spark.createDataFrame(df, schema = schema_tickers_eps)

    tickers_eps_sp_df = tickers_eps_sp_df.\
    withColumn( 'earnings_date'  ,col('Earnings Date')).\
    withColumn( 'eps_estimate' ,col('EPS Estimate').cast(DecimalType(20, 2))).\
    withColumn( 'reported_eps' ,col('Reported EPS').cast(DecimalType(20, 2))).\
    withColumn( 'surprise_ratio' ,col('Surprise(%)').cast(DecimalType(20, 2)))

    tickers_eps_sp_df = tickers_eps_sp_df.select('earnings_date','symbol','eps_estimate','reported_eps',\
    'surprise_ratio' )

    tickers_eps_sp_df.write.mode('overwrite').parquet(dump_file)
    tickers_eps_sp_df.show(truncate = False)
    
    
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

    print('get tickers earnings per share dates')    
    tickers_earnings_dates_ls = get_tickers_earnings_dates(tickers_ls,tickers)
    
    print('write tickers earnings per share dates parquet file') 
    write_tickers_earnings_dates_to_parquet(tickers_earnings_dates_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()