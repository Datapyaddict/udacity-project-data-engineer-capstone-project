
import os
import sys
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import time
import json
import configparser
from stocksymbol import StockSymbol

from common_modules import create_spark_session


def get_stocksymbol_data(spark, api_key, s3_bucket_prefix,\
                         sparkContext,sqlContext):
    """
    This function calls the list of stock markets via api from the stocksymbol module and writes the data back into
    json files per market in the s3 bucket.
    :param spark: spark object
    :param api_key: api key to use stocksymbol api.
    :param s3_bucket_prefix: the s3 bucket prefix that contains the files produced by the function
    :param sparkContext: sparkContext object
    :param sqlContext: sqlContext object
    :return: None
    """
    
    ss = StockSymbol(api_key)
    
    market_list = ss.market_list
    

    markets_static_data_path = os.path.join(s3_bucket_prefix,'markets_static_data/')

    market_indexes_path = os.path.join(markets_static_data_path,'market_indexes/')
    
    tickers_exchanges_path = os.path.join(markets_static_data_path,'tickers_exchanges/')


    print("dump markets indexes parquet file into input folder")
    
    dump_file = os.path.join(market_indexes_path,'markets_indexes.parquet')
    json_data = sparkContext.parallelize(market_list)
    markets_list_sp_df = sqlContext.read.json(json_data)
    markets_list_sp_df.write.mode('overwrite').parquet(dump_file)  
    
    print("dump tickers exchanges markets parquet file into input folder")
    
    for market in market_list:
        market = market['market']  
        market_exchange = ss.get_symbol_list(market=market) 
        json_data = sparkContext.parallelize(market_exchange)
        market_exchange_sp_df = sqlContext.read.json(json_data)
        dump_file = os.path.join(tickers_exchanges_path,f'tickers_exchanges_{market}.parquet')
        market_exchange_sp_df.write.mode('overwrite').parquet(dump_file)

    

def main():

    print('python version : ', sys.version)

    print("create spark session")
    
    spark = create_spark_session()
    
    print("read in the config file")

    config = configparser.ConfigParser()
    
    config.read('/home/hadoop/dl_config.cfg')
   
    STOCKSYMBOL_API_KEY    = config.get('APIS','STOCKSYMBOL_API_KEY')
    DL_BUCKET_INPUT_PREFIX = config.get('S3_BUCKET','DL_BUCKET_INPUT_PREFIX')
     
    print("connect to API")
    sparkContext=spark.sparkContext
    sc = sparkContext
    sqlContext = SQLContext(sc)
    get_stocksymbol_data(spark,STOCKSYMBOL_API_KEY,DL_BUCKET_INPUT_PREFIX,\
                         sparkContext,sqlContext)


if __name__ == "__main__":
    main()