
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


def get_tickers_earnings(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers earnings info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers earnings.
    """
    tickers_earnings_ls = []
    print('load yearly earnings')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            earnings = tickers.tickers[f"{tickers_ls[i]}"].earnings
            earnings = earnings.reset_index()
            earnings = earnings.rename(columns = {'Year': 'period_detail'})
            earnings = earnings.to_dict()
            earnings['symbol'] = tickers_ls[i]
            earnings['period'] = 'year'
            tickers_earnings_ls.append(earnings)
        except Exception as e:
            print(e)
            continue
            
    print('load quarterly earnings')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            earnings = tickers.tickers[f"{tickers_ls[i]}"].quarterly_earnings
            earnings = earnings.reset_index()
            earnings = earnings.rename(columns = {'Quarter': 'period_detail'})
            earnings = earnings.to_dict()
            earnings['symbol'] = tickers_ls[i]
            earnings['period'] = 'quarter'
            tickers_earnings_ls.append(earnings)

        except Exception as e:
            print(e)
            continue
            
    return tickers_earnings_ls

def write_tickers_earnings_to_parquet(tickers_earnings_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers earnings info and writes the data into a parquet file
    in input folder.
    :param tickers_earnings_ls: tickers earnings list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_tickers_earnings = StructType([ \
    StructField('period_detail',StringType(),True),
    StructField('revenue',DoubleType(),True), 
    StructField('earnings',DoubleType(),True),
    StructField('period',StringType(),True),
    StructField('symbol',StringType(),True)
      ])

    print('create dataframe of earnings for tickers')
    
    target_columns = ['period_detail','Revenue', 'Earnings',\
                                'period', 'symbol']
    
    df = pd.DataFrame([],columns = target_columns)
    
    for symbol in tickers_earnings_ls:
        try :
            symbol_df = pd.DataFrame(symbol)

            if len(symbol_df) ==0 : continue
        except Exception: 
            print(symbol)
            continue
        else:    
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)
    
    df['revenue'] = df['Revenue'].astype('Float64')
    df['earnings'] = df['Earnings'].astype('Float64')
    
    df = df[['period_detail', 'revenue','earnings','period', 'symbol']]

#     for column in df.columns:
#         if column not in target_columns:
#             df = df.drop(columns=column)  
            
    print('write tickers_earnings.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_earnings_path = os.path.join(markets_static_data_path,'tickers_earnings/')

    dump_file = os.path.join(tickers_earnings_path,'tickers_earnings.parquet')
    
    tickers_earnings_sp_df = spark.createDataFrame(df, schema = schema_tickers_earnings)

    tickers_earnings_sp_df = tickers_earnings_sp_df.\
    withColumn( 'revenue' ,col('revenue').cast(DecimalType(20, 2))).\
    withColumn( 'earnings' ,col('earnings').cast(DecimalType(20, 2)))

    tickers_earnings_sp_df = tickers_earnings_sp_df.select('period_detail','revenue', 'earnings',\
                                    'period', 'symbol')

    tickers_earnings_sp_df.write.mode('overwrite').parquet(dump_file)
    tickers_earnings_sp_df.show(truncate = False)
    
    
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

    print('get tickers earnings')    
    tickers_earnings_ls = get_tickers_earnings(tickers_ls,tickers)
    
    print('write tickers earnings info parquet file') 
    write_tickers_earnings_to_parquet(tickers_earnings_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()