
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


def get_tickers_financials(tickers_ls,tickers):
    """
    Based on the list of tickers tickers_ls and tickers object tickers, the function returns
    a list of all the tickers financials info via an api request.
    :param tickers_ls: list of tickers.
    :param tickers: tickers object.
    :return: the list of tickers financials.
    """
    ticker_financials_ls = []
    print('load yearly financials')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            financials = tickers.tickers[f"{tickers_ls[i]}"].financials
            financials.columns = financials.columns.astype('str') 
            financials = financials.T
            financials = financials.to_dict()
            financials['symbol'] = tickers_ls[i]
            financials['period'] = 'year'
            ticker_financials_ls.append(financials)

        except Exception as e:
            print(e)
            continue
            
    print('load quarterly financials')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        try:
            financials = tickers.tickers[f"{tickers_ls[i]}"].quarterly_financials
            financials.columns = financials.columns.astype('str') 
            financials = financials.T
            financials = financials.to_dict()
            financials['symbol'] = tickers_ls[i]
            financials['period'] = 'quarter'
            ticker_financials_ls.append(financials)

        except Exception as e:
            print(e)
            continue
            
    return ticker_financials_ls

def write_tickers_financials_to_parquet(ticker_financials_ls,DL_BUCKET_INPUT_PREFIX, spark):
    """
    The function creates a dataframe from the tickers financials info and writes the data into a parquet file
    in input folder.
    :param tickers_financials_ls: tickers financials list
    :param DL_BUCKET_INPUT_PREFIX: prefix of the input folder
    :param spark: spark object
    :return: None
    """
    schema_ticker_financials = StructType([ \
    StructField("date",StringType(),True),
    StructField("Research Development",DoubleType()),
    StructField("Effect Of Accounting Charges",DoubleType(),True),                    
    StructField("Income Before Tax",DoubleType(),True),
    StructField("Minority Interest",DoubleType(),True),
    StructField("Net Income",DoubleType(),True),
    StructField("Selling General Administrative",DoubleType(),True),
    StructField("Gross Profit",DoubleType(),True),
    StructField("Ebit",DoubleType(),True),
    StructField("Operating Income",DoubleType(),True),
    StructField("Other Operating Expenses",DoubleType(),True),
    StructField("Interest Expense",DoubleType(),True),
    StructField("Extraordinary Items",DoubleType(),True),
    StructField("Non Recurring",DoubleType(),True),
    StructField("Other Items",DoubleType(),True),
    StructField("Income Tax Expense",DoubleType(),True),
    StructField("Total Revenue",DoubleType(),True),
    StructField("Total Operating Expenses",DoubleType(),True),
    StructField("Cost Of Revenue",DoubleType(),True),
    StructField("Total Other Income Expense Net",DoubleType(),True),
    StructField("Discontinued Operations",DoubleType(),True),
    StructField("Net Income From Continuing Ops",DoubleType(),True),
    StructField("Net Income Applicable To Common Shares",DoubleType(),True),
    StructField("symbol",StringType(),True),
    StructField("period",StringType(),True)
      ])

    print('create dataframe of financials for tickers')
    
    target_columns = ['date','Research Development', 
                                    'Effect Of Accounting Charges',
           'Income Before Tax', 'Minority Interest', 'Net Income',
           'Selling General Administrative', 'Gross Profit', 'Ebit',
           'Operating Income', 'Other Operating Expenses', 'Interest Expense',
           'Extraordinary Items', 'Non Recurring', 'Other Items',
           'Income Tax Expense', 'Total Revenue', 'Total Operating Expenses',
           'Cost Of Revenue', 'Total Other Income Expense Net',
           'Discontinued Operations', 'Net Income From Continuing Ops',
           'Net Income Applicable To Common Shares', 'symbol','period']
    df = pd.DataFrame([],columns = target_columns)
    
    for symbol in ticker_financials_ls:
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
            
    print('write tickers_financials.parquet')
    
    markets_static_data_path = os.path.join(DL_BUCKET_INPUT_PREFIX,'markets_static_data/')
    tickers_financials_path = os.path.join(markets_static_data_path,'tickers_financials/')

    dump_file = os.path.join(tickers_financials_path,'tickers_financials.parquet')
    
    ticker_financials_sp_df = spark.createDataFrame(df, schema = schema_ticker_financials)
    ticker_financials_sp_df = ticker_financials_sp_df.\
        withColumn('research_development', col('Research Development').cast(DecimalType(20, 2))).\
        withColumn('effect_of_accounting_charges', col('Effect Of Accounting Charges').cast(DecimalType(20, 2))).\
        withColumn('income_before_tax', col('Income Before Tax').cast(DecimalType(18, 2))).\
        withColumn('minority_interest', col('Minority Interest').cast(DecimalType(20, 2))).\
        withColumn('net_income', col('Net Income').cast(DecimalType(20, 2))).\
        withColumn('selling_general_administrative', col('Selling General Administrative').cast(DecimalType(18, 2))).\
        withColumn('gross_profit', col('Gross Profit').cast(DecimalType(20, 2))).\
        withColumn('ebit', col('Ebit').cast(DecimalType(20, 2))).\
        withColumn('operating_income', col('Operating Income').cast(DecimalType(18, 2))).\
        withColumn('other_operating_expenses', col('Other Operating Expenses').cast(DecimalType(20, 2))).\
        withColumn('interest_expense', col('Interest Expense').cast(DecimalType(20, 2))).\
        withColumn('extraordinary_items', col('Extraordinary Items').cast(DecimalType(18, 2))).\
        withColumn('non_recurring', col('Non Recurring').cast(DecimalType(20, 2))).\
        withColumn('other_items', col('Other Items').cast(DecimalType(20, 2))).\
        withColumn('income_tax_expense', col('Income Tax Expense').cast(DecimalType(18, 2))).\
        withColumn('total_revenue', col('Total Revenue').cast(DecimalType(20, 2))).\
        withColumn('total_operating_expenses', col('Total Operating Expenses').cast(DecimalType(20, 2))).\
        withColumn('cost_of_revenue', col('Cost Of Revenue').cast(DecimalType(20, 2))).\
        withColumn('total_other_income_expense_net', col('Total Other Income Expense Net').cast(DecimalType(20, 2))).\
        withColumn('discontinued_operations', col('Discontinued Operations').cast(DecimalType(20, 2))).\
        withColumn('net_income_from_continuing_ops', col('Net Income From Continuing Ops').cast(DecimalType(20, 2))).\
        withColumn('net_income_applicable_to_common_shares', col('Net Income Applicable To Common Shares').cast(DecimalType(20,\
            2)))

    ticker_financials_sp_df = ticker_financials_sp_df.select('date','symbol','period','research_development','effect_of_accounting_charges',
                     'income_before_tax','minority_interest','net_income','selling_general_administrative',
                    'gross_profit','ebit','operating_income','other_operating_expenses','interest_expense',
                     'extraordinary_items','non_recurring','other_items','income_tax_expense','total_revenue',
                     'total_operating_expenses','cost_of_revenue','total_other_income_expense_net','discontinued_operations',
                     'net_income_from_continuing_ops','net_income_applicable_to_common_shares')

    ticker_financials_sp_df.write.mode('overwrite').parquet(dump_file)
    ticker_financials_sp_df.show(truncate = False)
    
    
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

    print('get tickers financials')    
    ticker_financials_ls = get_tickers_financials(tickers_ls,tickers)
    
    print('write tickers financial info parquet file') 
    write_tickers_financials_to_parquet(ticker_financials_ls, DL_BUCKET_INPUT_PREFIX, spark)
    

if __name__ == "__main__":
    main()