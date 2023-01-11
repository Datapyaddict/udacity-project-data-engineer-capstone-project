
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


def get_tickers_recommendations(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers recommendations info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers recommendations.
    """
    tickers_recommendations_ls = []
    print('load tickers recommendations')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            recommendations = yf.Ticker(f"{tickers_ls[i]}").recommendations
            recommendations = recommendations.reset_index()
            recommendations['Date'] = recommendations['Date'].dt.strftime('%Y-%m-%d')
            recommendations = recommendations.to_dict()
            recommendations['symbol'] = tickers_ls[i]
            tickers_recommendations_ls.append(recommendations)

        except Exception as e:
            print(e)
            continue
            
            
    return tickers_recommendations_ls

def write_tickers_recommendations_to_parquet(tickers_recommendations_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers recommendations and writes the data into a parquet file
    in input folder.
    :param tickers_recommendations_ls: tickers recommendations list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_recommendations = StructType([\
    StructField('Date',StringType(),True),\
    StructField('Firm',StringType(),True),\
    StructField('To Grade',StringType(),True),\
    StructField('From Grade',StringType(),True),\
    StructField('Action',StringType(),True),\
    StructField('symbol',StringType(),True)
                ])

    print('create dataframe of recommendations for tickers')
    
    target_columns = ['Date', 'Firm', 'To Grade', 'From Grade', 'Action', 'symbol']
    
    df = pd.DataFrame([],columns = target_columns)

    for symbol in tickers_recommendations_ls:
        symbol_df = pd.DataFrame(symbol)[['Date', 'Firm', 'To Grade', 'From Grade', 'Action', 'symbol']]
        if len(symbol_df) ==0 :
            print(symbol)
            continue
        
        else :
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column)        
            
    print('write tickers_recommendations.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_recommendations_path = os.path.join(markets_static_data_path,'tickers_recommendations/')

    dump_file = os.path.join(tickers_recommendations_path,'tickers_recommendations.parquet')
    print(df.columns)
    print(df.shape)
    
    ticker_recommendations_sp_df = spark.createDataFrame(df, schema = schema_ticker_recommendations)

    ticker_recommendations_sp_df = ticker_recommendations_sp_df.\
        withColumn( 'date'  ,col('Date')).\
        withColumn( 'firm'  ,col('Firm')).\
        withColumn( 'to_grade'  ,col('To Grade')).\
        withColumn( 'from_grade'  ,col('From Grade')).\
        withColumn( 'action'  ,col('Action'))

    ticker_recommendations_sp_df = ticker_recommendations_sp_df.\
            select('date', 'firm','from_grade', 'to_grade', 'action','symbol')
    ticker_recommendations_sp_df.write.mode('overwrite').parquet(dump_file)
    ticker_recommendations_sp_df.show(truncate = False)
    
    
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

    print('get tickers recommendations')    
    tickers_recommendations_ls = get_tickers_recommendations(tickers_ls,tickers)
    
    print('write tickers recommendationsw info parquet file') 
    write_tickers_recommendations_to_parquet(tickers_recommendations_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()
