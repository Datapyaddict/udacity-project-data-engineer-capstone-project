
import pandas as pd
import os
import sys
# from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , DoubleType ,\
                                StringType , IntegerType , DateType ,\
                                TimestampType,DecimalType,LongType

from pyspark.sql.functions import col

import configparser
import yfinance as yf  

from common_modules import create_spark_session
from common_modules import get_tickers_ls
from common_modules import get_tickers_object




def get_tickers_analysis(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers analysis info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers analysis.
    """
    tickers_analysis_ls = []
    print('load tickers analysis')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            analysis = tickers.tickers[f"{tickers_ls[i]}"].earnings_trend
            analysis = analysis.reset_index()
            analysis['End Date'] = analysis['End Date'].dt.strftime('%Y-%m-%d')
            analysis = analysis.to_dict()
            analysis['symbol'] = tickers_ls[i]
            tickers_analysis_ls.append(analysis)

        except Exception as e:
            print(e)
            continue
                        
    return tickers_analysis_ls

def write_tickers_analysis_to_parquet(tickers_analysis_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers analysis info and writes the data into a parquet file
    in input folder.
    :param tickers_analysis_ls: tickers analysis list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_analysis = StructType([\
    StructField('Period',StringType(),True),\
    StructField('Max Age',IntegerType(),True),\
    StructField('End Date',StringType(),True),\
    StructField('Growth',DoubleType(),True),\
    StructField('Earnings Estimate Avg',DoubleType(),True),\
    StructField('Earnings Estimate Low',DoubleType(),True),\
    StructField('Earnings Estimate High',DoubleType(),True),\
    StructField('Earnings Estimate Year Ago Eps',DoubleType(),True),\
    StructField('Earnings Estimate Number Of Analysts',DoubleType(),True),\
    StructField('Earnings Estimate Growth',DoubleType(),True),\
    StructField('Revenue Estimate Avg',DoubleType(),True),\
    StructField('Revenue Estimate Low',DoubleType(),True),\
    StructField('Revenue Estimate High',DoubleType(),True),\
    StructField('Revenue Estimate Number Of Analysts',DoubleType(),True),\
    StructField('Revenue Estimate Year Ago Revenue',DoubleType(),True),\
    StructField('Revenue Estimate Growth',DoubleType(),True),\
    StructField('Eps Trend Current',DoubleType(),True),\
    StructField('Eps Trend 7Days Ago',DoubleType(),True),\
    StructField('Eps Trend 30Days Ago',DoubleType(),True),\
    StructField('Eps Trend 60Days Ago',DoubleType(),True),\
    StructField('Eps Trend 90Days Ago',DoubleType(),True),\
    StructField('Eps Revisions Up Last7Days',DoubleType(),True),\
    StructField('Eps Revisions Up Last30Days',DoubleType(),True),\
    StructField('Eps Revisions Down Last30Days',DoubleType(),True),\
    StructField('Eps Revisions Down Last90Days',DoubleType(),True),\
    StructField('symbol',StringType(),True)
                ])

    print('create dataframe of tickers analysis')
    
    target_columns = ['Period', 'Max Age', 'End Date', 'Growth', 
                                'Earnings Estimate Avg', 'Earnings Estimate Low', 
                                'Earnings Estimate High', 'Earnings Estimate Year Ago Eps', 
                                'Earnings Estimate Number Of Analysts', 'Earnings Estimate Growth', 
                                'Revenue Estimate Avg', 'Revenue Estimate Low', 'Revenue Estimate High', 
                                'Revenue Estimate Number Of Analysts', 'Revenue Estimate Year Ago Revenue', 
                                'Revenue Estimate Growth', 'Eps Trend Current', 'Eps Trend 7Days Ago', 
                                'Eps Trend 30Days Ago', 'Eps Trend 60Days Ago', 'Eps Trend 90Days Ago', 
                                'Eps Revisions Up Last7Days', 'Eps Revisions Up Last30Days', 
                                'Eps Revisions Down Last30Days', 'Eps Revisions Down Last90Days', 'symbol']
    
    df = pd.DataFrame([],columns = target_columns)

    for symbol in tickers_analysis_ls:
        symbol_df = pd.DataFrame(symbol)
        columns = [x for x in symbol.keys()]
        if len(symbol_df) ==0 :
            print(symbol)
            continue

        else :
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    df['Max Age'] = df['Max Age'].astype('int64')
    df['Growth'] = df['Growth'].astype('float64')
    df['Earnings Estimate Avg'] = df['Earnings Estimate Avg'].astype('float64')
    df['Earnings Estimate Low'] = df['Earnings Estimate Low'].astype('float64')
    df['Earnings Estimate High'] = df['Earnings Estimate High'].astype('float64')
    df['Earnings Estimate Year Ago Eps'] = df['Earnings Estimate Year Ago Eps'].astype('float64')
    df['Earnings Estimate Number Of Analysts'] = df['Earnings Estimate Number Of Analysts'].astype('float64')
    df['Earnings Estimate Growth'] = df['Earnings Estimate Growth'].astype('float64')
    df['Revenue Estimate Avg'] = df['Revenue Estimate Avg'].astype('float64')
    df['Revenue Estimate Low'] = df['Revenue Estimate Low'].astype('float64')
    df['Revenue Estimate High'] = df['Revenue Estimate High'].astype('float64')
    df['Revenue Estimate Number Of Analysts'] = df['Revenue Estimate Number Of Analysts'].astype('float64')
    df['Revenue Estimate Year Ago Revenue'] = df['Revenue Estimate Year Ago Revenue'].astype('float64')
    df['Revenue Estimate Growth'] = df['Revenue Estimate Growth'].astype('float64')
    df['Eps Trend Current'] = df['Eps Trend Current'].astype('float64')
    df['Eps Trend 7Days Ago'] = df['Eps Trend 7Days Ago'].astype('float64')
    df['Eps Trend 30Days Ago'] = df['Eps Trend 30Days Ago'].astype('float64')
    df['Eps Trend 60Days Ago'] = df['Eps Trend 60Days Ago'].astype('float64')
    df['Eps Trend 90Days Ago'] = df['Eps Trend 90Days Ago'].astype('float64')
    df['Eps Revisions Up Last7Days'] = df['Eps Revisions Up Last7Days'].astype('float64')
    df['Eps Revisions Up Last30Days'] = df['Eps Revisions Up Last30Days'].astype('float64')
    df['Eps Revisions Down Last30Days'] = df['Eps Revisions Down Last30Days'].astype('float64')
    df['Eps Revisions Down Last90Days'] = df['Eps Revisions Down Last90Days'].astype('float64')       
            
    print('write ticker_analysis.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_analysis_path = os.path.join(markets_static_data_path,'tickers_analysis/')

    dump_file = os.path.join(tickers_analysis_path,'tickers_analysis.parquet')
    print(df.columns)
    print(df.shape)
    
    ticker_analysis_sp_df = spark.createDataFrame(df, schema = schema_ticker_analysis)

    ticker_analysis_sp_df = ticker_analysis_sp_df.\
    withColumn( 'period'  ,col('Period')).\
    withColumn( 'max_age'  ,col('Max Age')).\
    withColumn( 'growth'  ,col('Growth')).\
    withColumn( 'earnings_estimate_avg' ,col('Earnings Estimate Avg').cast(DecimalType(20, 2))).\
    withColumn( 'earnings_estimate_low' ,col('Earnings Estimate Low').cast(DecimalType(20, 2))).\
    withColumn( 'earnings_estimate_high' ,col('Earnings Estimate High').cast(DecimalType(20, 2))).\
    withColumn( 'earnings_estimate_year_ago_eps' ,col('Earnings Estimate Year Ago Eps').cast(DecimalType(20, 2))).\
    withColumn( 'earnings_estimate_number_of_analysts' ,col('Earnings Estimate Number Of Analysts').cast(DecimalType(20, 2))).\
    withColumn( 'earnings_estimate_growth' ,col('Earnings Estimate Growth').cast(DecimalType(20, 2))).\
    withColumn( 'revenue_estimate_avg' ,col('Revenue Estimate Avg').cast(DecimalType(20, 2))).\
    withColumn( 'revenue_estimate_low' ,col('Revenue Estimate Low').cast(DecimalType(20, 2))).\
    withColumn( 'revenue_estimate_high' ,col('Revenue Estimate High').cast(DecimalType(20, 2))).\
    withColumn( 'eps_trend_current' ,col('Eps Trend Current').cast(DecimalType(20, 2))).\
    withColumn( 'eps_trend_7days_ago' ,col('Eps Trend 7Days Ago').cast(DecimalType(20, 2))).\
    withColumn( 'eps_trend_30days_ago' ,col('Eps Trend 30Days Ago').cast(DecimalType(20, 2))).\
    withColumn( 'eps_trend_60days_ago' ,col('Eps Trend 60Days Ago').cast(DecimalType(20, 2))).\
    withColumn( 'eps_trend_90days_ago' ,col('Eps Trend 90Days Ago').cast(DecimalType(20, 2))).\
    withColumn( 'eps_revisions_up_last7days' ,col('Eps Revisions Up Last7Days').cast(DecimalType(20, 2))).\
    withColumn( 'eps_revisions_up_last30days' ,col('Eps Revisions Up Last30Days').cast(DecimalType(20, 2))).\
    withColumn( 'eps_revisions_down_last30days' ,col('Eps Revisions Down Last30Days').cast(DecimalType(20, 2))).\
    withColumn( 'eps_revisions_down_last90days' ,col('Eps Revisions Down Last90Days').cast(DecimalType(20, 2)))




    ticker_analysis_sp_df = ticker_analysis_sp_df.\
            select('period','growth','earnings_estimate_avg' ,'earnings_estimate_low' ,
    'earnings_estimate_high' ,'earnings_estimate_year_ago_eps' ,
    'earnings_estimate_number_of_analysts' ,'earnings_estimate_growth','revenue_estimate_avg' ,
    'revenue_estimate_low','revenue_estimate_high' ,'eps_trend_current' ,'eps_trend_7days_ago' ,
    'eps_trend_30days_ago' ,'eps_trend_60days_ago' ,'eps_trend_90days_ago' ,'eps_revisions_up_last7days' ,
    'eps_revisions_up_last30days','eps_revisions_down_last30days' ,'eps_revisions_down_last90days' ,'symbol' )

    ticker_analysis_sp_df.write.mode('overwrite').parquet(dump_file)
    ticker_analysis_sp_df.show(truncate = False)
    
    
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

    print('get tickers anaysis')    
    tickers_analysis_ls = get_tickers_analysis(tickers_ls,tickers)
    
    print('write tickers analysis info parquet file') 
    write_tickers_analysis_to_parquet(tickers_analysis_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()
