
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


def get_tickers_balancesheet(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers balancesheet info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers balancesheet.
    """
    tickers_balancesheet_ls = []
    print('load yearly balancesheet')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            balancesheet = tickers.tickers[f"{tickers_ls[i]}"].balancesheet
            balancesheet.columns = balancesheet.columns.astype('str') 
            balancesheet = balancesheet.T
            balancesheet = balancesheet.to_dict()
            balancesheet['symbol'] = tickers_ls[i]
            balancesheet['period'] = 'year'
            tickers_balancesheet_ls.append(balancesheet)

        except Exception as e:
            print(e)
            continue
            
    print('load quarterly balancesheet')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            balancesheet = tickers.tickers[f"{tickers_ls[i]}"].quarterly_balance_sheet
            balancesheet.columns = balancesheet.columns.astype('str') 
            balancesheet = balancesheet.T
            balancesheet = balancesheet.to_dict()
            balancesheet['symbol'] = tickers_ls[i]
            balancesheet['period'] = 'quarter'
            tickers_balancesheet_ls.append(balancesheet)

        except Exception as e:
            print(e)
            continue
            
    return tickers_balancesheet_ls

def write_tickers_balancesheet_to_parquet(tickers_balancesheet_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers balancesheet info and writes the data into a parquet file
    in input folder.
    :param tickers_balancesheet_ls: tickers balancesheet list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_tickers_balancesheet = StructType([ 
    StructField("date",StringType(),True),
    StructField("Intangible Assets",DoubleType()),
    StructField("Capital Surplus",DoubleType(),True),                    
    StructField("Total Liab",DoubleType(),True),
    StructField("Total Stockholder Equity",DoubleType(),True),
    StructField("Minority Interest",DoubleType(),True),
    StructField("Other Current Liab",DoubleType(),True),
    StructField("Total Assets",DoubleType(),True),
    StructField("Common Stock",DoubleType(),True),
    StructField("Other Current Assets",DoubleType(),True),
    StructField("Retained Earnings",DoubleType(),True),
    StructField("Other Liab",DoubleType(),True),
    StructField("Good Will",DoubleType(),True),
    StructField("Treasury Stock",DoubleType(),True),
    StructField("Other Assets",DoubleType(),True),
    StructField("Cash",DoubleType(),True),
    StructField("Total Current Liabilities",DoubleType(),True),
    StructField("Deferred Long Term Asset Charges",DoubleType(),True),
    StructField("Short Long Term Debt",DoubleType(),True),
    StructField("Other Stockholder Equity",DoubleType(),True),
    StructField("Property Plant Equipment",DoubleType(),True),
    StructField("Total Current Assets",DoubleType(),True),
    StructField("Long Term Investments",DoubleType(),True),
    StructField("Net Tangible Asset",DoubleType(),True),
    StructField("Short Term Investments",DoubleType(),True),
    StructField("Net Receivables",DoubleType(),True),
    StructField("Long Term Debt",DoubleType(),True),
    StructField("Inventory",DoubleType(),True),
    StructField("Accounts Payable",DoubleType(),True),
    StructField("symbol",StringType(),True),
    StructField("period",StringType(),True),                                          
    StructField("Deferred Long Term Liab",DoubleType(),True),
      ])

    print('create dataframe of balancesheet for tickers')
    
    target_columns = ['date','Intangible Assets', 
                            'Capital Surplus',
   'Total Liab', 'Total Stockholder Equity', 'Minority Interest',
   'Other Current Liab',"Total Assets","Common Stock","Other Current Assets",
    "Retained Earnings", "Other Liab" ,"Good Will","Treasury Stock", "Other Assets","Cash",                   
    "Total Current Liabilities", "Deferred Long Term Asset Charges","Short Long Term Debt",                     
    "Other Stockholder Equity","Property Plant Equipment" ,"Total Current Assets", "Long Term Investments",
     "Net Tangible Asset", "Short Term Investments", "Net Receivables","Long Term Debt",                    
      "Inventory", "Accounts Payable", "symbol","period",'Deferred Long Term Liab']
    
    df = pd.DataFrame([],columns = target_columns)

    for symbol in tickers_balancesheet_ls:
        try :
            symbol_df = pd.DataFrame(symbol).reset_index().rename(columns = {'index':'date'})
        except Exception: 
            print(symbol)
            continue
        else:    
            df = pd.concat([df,symbol_df],  axis = 0,ignore_index = True)

    for column in df.columns:
        if column not in target_columns:
            df = df.drop(columns=column)        
            
    print('write tickers_balancesheet.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_balancesheet_path = os.path.join(markets_static_data_path,'tickers_balancesheet/')

    dump_file = os.path.join(tickers_balancesheet_path,'tickers_balancesheet.parquet')
    print(df.columns)
    print(df.shape)
    
    tickers_balancesheet_sp_df = spark.createDataFrame(df, schema = schema_tickers_balancesheet)
    tickers_balancesheet_sp_df = tickers_balancesheet_sp_df.\
            withColumn('intangible_assets', col('Intangible Assets').cast(DecimalType(20, 2))).\
            withColumn('capital_surplus', col('Capital Surplus').cast(DecimalType(20, 2))).\
            withColumn('total_liab', col('Total Liab').cast(DecimalType(18, 2))).\
            withColumn('total_stockholder_equity', col('Total Stockholder Equity').cast(DecimalType(20, 2))).\
            withColumn('minority_interest', col('Minority Interest').cast(DecimalType(20, 2))).\
            withColumn('other_current_liab', col('Other Current Liab').cast(DecimalType(18, 2))).\
            withColumn('total_assets', col('Total Assets').cast(DecimalType(20, 2))).\
            withColumn('common_stock', col('Common Stock').cast(DecimalType(20, 2))).\
            withColumn('other_current_assets', col('Other Current Assets').cast(DecimalType(18, 2))).\
            withColumn('retained_earnings', col('Retained Earnings').cast(DecimalType(20, 2))).\
            withColumn('other_liab', col('Other Liab').cast(DecimalType(20, 2))).\
            withColumn('good_will', col('Good Will').cast(DecimalType(18, 2))).\
            withColumn('treasury_stock', col('Treasury Stock').cast(DecimalType(20, 2))).\
            withColumn('other_assets', col('Other Assets').cast(DecimalType(20, 2))).\
            withColumn('cash', col('Cash').cast(DecimalType(18, 2))).\
            withColumn('total_current_liabilities', col('Total Current Liabilities').cast(DecimalType(20, 2))).\
            withColumn('deferred_long_term_asset_charges', col('Deferred Long Term Asset Charges').cast(DecimalType(20, 2))).\
            withColumn('short_long_term_debt', col('Short Long Term Debt').cast(DecimalType(20, 2))).\
            withColumn('other_stockholder_equity', col('Other Stockholder Equity').cast(DecimalType(20, 2))).\
            withColumn('property_plant_equipment', col('Property Plant Equipment').cast(DecimalType(20, 2))).\
            withColumn('total_current_assets', col('Total Current Assets').cast(DecimalType(20, 2))).\
          withColumn('long_term_investments', col('Long Term Investments').cast(DecimalType(20, 2))).\
          withColumn('net_tangible_asset', col('Net Tangible Asset').cast(DecimalType(20, 2))).\
          withColumn('short_term_investments', col('Short Term Investments').cast(DecimalType(20, 2))).\
          withColumn('net_receivables', col('Net Receivables').cast(DecimalType(20, 2))).\
          withColumn('long_term_debt', col('Long Term Debt').cast(DecimalType(20, 2))).\
          withColumn('inventory', col('Inventory').cast(DecimalType(20, 2))).\
          withColumn('deferred_long_term_liab', col('Deferred Long Term Liab').cast(DecimalType(20, 2))).\
          withColumn('accounts_payable', col('Accounts Payable').cast(DecimalType(20, 2)))

    tickers_balancesheet_sp_df = tickers_balancesheet_sp_df.select('date','symbol','period','intangible_assets',\
       'capital_surplus','total_liab','total_stockholder_equity', 'minority_interest','other_current_liab',\
     'total_assets', 'common_stock', 'other_current_assets','retained_earnings','other_liab', 'good_will',\
      'treasury_stock','other_assets', 'cash', 'total_current_liabilities','deferred_long_term_asset_charges',\
      'short_long_term_debt','other_stockholder_equity', 'property_plant_equipment', 'total_current_assets',\
       'long_term_investments', 'net_tangible_asset', 'short_term_investments', 'net_receivables',\
       'long_term_debt','inventory','deferred_long_term_liab','accounts_payable')

    tickers_balancesheet_sp_df.write.mode('overwrite').parquet(dump_file)
    tickers_balancesheet_sp_df.show(truncate = False)
    
    
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

    print('get tickers balancesheet')    
    tickers_balancesheet_ls = get_tickers_balancesheet(tickers_ls,tickers)
    
    print('write tickers balancesheet info parquet file') 
    write_tickers_balancesheet_to_parquet(tickers_balancesheet_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()