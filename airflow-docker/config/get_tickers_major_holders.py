
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


def get_tickers_major_holders(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers major holders info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers major holders.
    """
    tickers_major_holders_ls = []
    print('load yearly actions')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            major_holders = tickers.tickers[f"{tickers_ls[i]}"].major_holders
            data = major_holders[0].tolist()
            columns =  major_holders[1].tolist()
            major_holders = pd.DataFrame([data], columns = columns)
            major_holders = major_holders.to_dict()
            major_holders['symbol'] = tickers_ls[i]
            tickers_major_holders_ls.append(major_holders)

        except Exception as e:
            print(e)
            continue
            
            
    return tickers_major_holders_ls

def write_tickers_major_holders_to_parquet(tickers_major_holders_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers major holders info and writes the data into a parquet file
    in input folder.
    :param tickers_major_holders_ls: tickers major holders list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_major_holders = StructType([ \
    StructField('% of Shares Held by All Insider',DoubleType(),True),
    StructField('% of Shares Held by Institutions',DoubleType(),True), 
    StructField('% of Float Held by Institutions',DoubleType(),True),
    StructField('Number of Institutions Holding Shares',IntegerType(),True),
    StructField('symbol',StringType(),True)
      ])

    print('create dataframe of major holders for tickers')
    
    target_columns = ['% of Shares Held by All Insider','% of Shares Held by Institutions',\
                                '% of Float Held by Institutions', 'Number of Institutions Holding Shares',\
                               'symbol']
    
    df = pd.DataFrame([],columns = target_columns)
    
    for symbol in tickers_major_holders_ls:
        symbol_df = pd.DataFrame(symbol)
        col_0 = [x for x in symbol.keys()][0]
        if len(symbol_df) ==0 :
            print(symbol)
            continue

        elif type(col_0) != str:    
            print(symbol)
            continue
        else :
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column)  
            
    print('write tickers_actions.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_major_holders_path = os.path.join(markets_static_data_path,'tickers_major_holders/')

    dump_file = os.path.join(tickers_major_holders_path,'tickers_major_holders.parquet')
    
    df = df.apply(lambda x:x.str.strip('%'), axis = 1)
    df['% of Shares Held by All Insider'] = df['% of Shares Held by All Insider'].astype('float64')
    df['% of Shares Held by Institutions'] = df['% of Shares Held by Institutions'].astype('float64')
    df['% of Float Held by Institutions'] = df['% of Float Held by Institutions'].astype('float64')
    df['Number of Institutions Holding Shares'] = df['Number of Institutions Holding Shares'].astype('int64')

    tickers_major_holders_sp_df = spark.createDataFrame(df, schema = schema_ticker_major_holders)

    tickers_major_holders_sp_df = tickers_major_holders_sp_df.\
    withColumn( 'percentage_of_shares_held_by_all_insider'  ,col('% of Shares Held by All Insider').cast(DecimalType(20, 2))).\
    withColumn( 'percentage_of_shares_held_by_institutions' ,col('% of Shares Held by Institutions').cast(DecimalType(20, 2))).\
    withColumn( 'percentage_of_float_held_by_institutions'  ,col( '% of Float Held by Institutions').cast(DecimalType(20, 2))).\
    withColumn( 'number_of_institutions_holding_shares' ,col('Number of Institutions Holding Shares'))

    tickers_major_holders_sp_df = tickers_major_holders_sp_df.select('symbol','percentage_of_shares_held_by_all_insider',\
             'percentage_of_shares_held_by_institutions','percentage_of_float_held_by_institutions',\
            'number_of_institutions_holding_shares' )

    tickers_major_holders_sp_df.write.mode('overwrite').parquet(dump_file)
    tickers_major_holders_sp_df.show(truncate = False)
    
    
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

    print('get tickers major holders')    
    tickers_major_holders_ls = get_tickers_major_holders(tickers_ls,tickers)
    
    print('write tickers major holders info parquet file') 
    write_tickers_major_holders_to_parquet(tickers_major_holders_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()