
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

def get_tickers_cashflow(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers cashflow info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers cashflow.
    """
    tickers_cashflow_ls = []
    print('load yearly cashflow')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            cashflow = tickers.tickers[f"{tickers_ls[i]}"].cashflow
            cashflow.columns = cashflow.columns.astype('str') 
            cashflow = cashflow.T
            cashflow = cashflow.to_dict()
            cashflow['symbol'] = tickers_ls[i]
            cashflow['period'] = 'year'
            tickers_cashflow_ls.append(cashflow)

        except Exception as e:
            print(e)
            continue
            
    print('load quarterly cashflow')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            cashflow = tickers.tickers[f"{tickers_ls[i]}"].quarterly_cashflow
            cashflow.columns = cashflow.columns.astype('str') 
            cashflow = cashflow.T
            cashflow = cashflow.to_dict()
            cashflow['symbol'] = tickers_ls[i]
            cashflow['period'] = 'quarter'
            tickers_cashflow_ls.append(cashflow)

        except Exception as e:
            print(e)
            continue
            
    return tickers_cashflow_ls

def write_tickers_cashflow_to_parquet(tickers_cashflow_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers cashflow info and writes the data into a parquet file
    in input folder.
    :param tickers_cashflow_ls: tickers cashflow list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_tickers_cashflow = StructType([ \
    StructField('date',StringType(),True),
    StructField('Investments',DoubleType(),True), 
    StructField('Change To Liabilities',DoubleType(),True),
    StructField('Total Cashflows From Investing Activities',DoubleType(),True), 
    StructField('Net Borrowings',DoubleType(),True),
    StructField('Total Cash From Financing Activities',DoubleType(),True), 
    StructField('Issuance Of Stock',DoubleType(),True), 
    StructField('Net Income',DoubleType(),True), 
    StructField('Change In Cash',DoubleType(),True), 
    StructField('Repurchase Of Stock',DoubleType(),True),
    StructField('Effect Of Exchange Rate',DoubleType(),True), 
    StructField('Total Cash From Operating Activities',DoubleType(),True), 
    StructField('Depreciation',DoubleType(),True), 
    StructField('Other Cashflows From Investing Activities',DoubleType(),True),
    StructField('Dividends Paid',DoubleType(),True),
    StructField('Change To Inventory',DoubleType(),True), 
    StructField('Change To Account Receivables',DoubleType(),True), 
    StructField('Other Cashflows From Financing Activities',DoubleType(),True),
    StructField('Change To Netincome',DoubleType(),True),
    StructField('Capital Expenditures',DoubleType(),True),
    StructField('symbol',StringType(),True),
    StructField('period',StringType(),True),
    StructField('Change To Operating Activities',DoubleType(),True)
      ])

    print('create dataframe of cashflow for tickers')
    
    target_columns = ['date','Investments', 'Change To Liabilities', 'Total Cashflows From Investing Activities',\
                      'Net Borrowings', 'Total Cash From Financing Activities', 'Issuance Of Stock', 'Net Income',\
                      'Change In Cash', 'Repurchase Of Stock', 'Effect Of Exchange Rate',\
                      'Total Cash From Operating Activities', 'Depreciation',\
                      'Other Cashflows From Investing Activities', 'Dividends Paid', 'Change To Inventory',\
                      'Change To Account Receivables', 'Other Cashflows From Financing Activities',\
                      'Change To Netincome', 'Capital Expenditures', 'symbol','period','Change To Operating Activities']
    
    df = pd.DataFrame([],columns = target_columns)

    for symbol in tickers_cashflow_ls:
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
    tickers_cashflow_path = os.path.join(markets_static_data_path,'tickers_cashflow/')

    dump_file = os.path.join(tickers_cashflow_path,'tickers_cashflow.parquet')
    print(df.columns)
    print(df.shape)
    
    tickers_cashflow_sp_df = spark.createDataFrame(df, schema = schema_tickers_cashflow)

    tickers_cashflow_sp_df = tickers_cashflow_sp_df.\
    withColumn( 'investments'  ,col('Investments').cast(DecimalType(20, 2))).\
    withColumn( 'change_to_liabilities' ,col('Change To Liabilities').cast(DecimalType(20, 2))).\
    withColumn( 'total_cashflows_from_investing_activities' ,\
               col('Total Cashflows From Investing Activities').cast(DecimalType(20, 2))).\
    withColumn( 'net_borrowings' ,col('Net Borrowings').cast(DecimalType(20, 2))).\
    withColumn( 'total_cash_from_financing_activities' ,col('Total Cash From Financing Activities').cast(DecimalType(20, 2))).\
    withColumn( 'issuance_of_stock' ,col('Issuance Of Stock').cast(DecimalType(20, 2))).\
    withColumn( 'net_income' ,col('Net Income').cast(DecimalType(20, 2))).\
    withColumn( 'change_in_cash' ,col('Change In Cash').cast(DecimalType(20, 2))).\
    withColumn( 'repurchase_of_stock' ,col('Repurchase Of Stock').cast(DecimalType(20, 2))).\
    withColumn( 'effect_of_exchange_rate' ,col('Effect Of Exchange Rate').cast(DecimalType(20, 2))).\
    withColumn( 'total_cash_from_operating_activities' ,col('Total Cash From Operating Activities').cast(DecimalType(20, 2))).\
    withColumn( 'depreciation' ,col('Depreciation').cast(DecimalType(20, 2))).\
    withColumn( 'other_cashflows_from_investing_activities' ,\
               col('Other Cashflows From Investing Activities').cast(DecimalType(20, 2))).\
    withColumn( 'dividends_paid' ,col('Dividends Paid').cast(DecimalType(20, 2))).\
    withColumn( 'change_to_inventory' ,col('Change To Inventory').cast(DecimalType(20, 2))).\
    withColumn( 'change_to_account_receivables' ,col('Change To Account Receivables').cast(DecimalType(20, 2))).\
    withColumn( 'other_cashflows_from_financing_activities' ,\
               col('Other Cashflows From Financing Activities').cast(DecimalType(20, 2))).\
    withColumn( 'change_to_net_income' ,col('Change To Netincome').cast(DecimalType(20, 2))).\
    withColumn( 'capital_expenditures' ,col('Capital Expenditures').cast(DecimalType(20, 2))).\
    withColumn( 'change_to_operating_activities' ,col('Change To Operating Activities').cast(DecimalType(20, 2)))


    tickers_cashflow_sp_df = tickers_cashflow_sp_df.select('date','symbol','period','investments',\
    'change_to_liabilities' ,'total_cashflows_from_investing_activities','net_borrowings' ,\
    'total_cash_from_financing_activities' ,'issuance_of_stock' ,'net_income' ,'change_in_cash' ,\
    'repurchase_of_stock' ,'effect_of_exchange_rate' ,'total_cash_from_operating_activities' ,\
    'depreciation' ,'other_cashflows_from_investing_activities' ,'dividends_paid' ,\
    'change_to_inventory' ,'change_to_account_receivables' ,'other_cashflows_from_financing_activities' ,\
    'change_to_net_income' ,'capital_expenditures','change_to_operating_activities')

    tickers_cashflow_sp_df.write.mode('overwrite').parquet(dump_file)
    tickers_cashflow_sp_df.show(truncate = False)
    
    
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

    print('get tickers cashflow')    
    tickers_cashflow_ls = get_tickers_cashflow(tickers_ls,tickers)
    
    print('write tickers cashflow info parquet file') 
    write_tickers_cashflow_to_parquet(tickers_cashflow_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()