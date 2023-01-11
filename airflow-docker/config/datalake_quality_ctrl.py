
import pandas as pd
import os
import configparser
from common_modules import create_spark_session
from pyspark.sql.functions import expr, col


dim_table_name_ls = [
                     {   'dim_table_name' :'financial_report_dim','key_columns' : ['reporting_id']},
                     {   'dim_table_name' :'hashtags_dim','key_columns' : ['hashtag_id','tweet_id','hashtag']},
                     {   'dim_table_name' :'holders_dim','key_columns' : ['holder','holder_type']},
                     {   'dim_table_name' :'location_dim','key_columns' : ['zip']},
                     {   'dim_table_name' :'news_dim','key_columns' : ['uuid']},
                     {   'dim_table_name' :'sector_dim','key_columns' : ['industry']},
                     {   'dim_table_name' :'tickers_news_fact','key_columns' : ['uuid','ticker']},
                     {   'dim_table_name' :'tickers_actions_fact','key_columns' : ['id']},
                     {   'dim_table_name' :'tickers_analysis_fact','key_columns' : ['ticker','period']},
                     {   'dim_table_name' :'tickers_balancesheet_fact','key_columns' : ['reporting_id']},
                     {   'dim_table_name' :'tickers_cashflow_fact','key_columns' : ['reporting_id']},
                     {   'dim_table_name' :'tickers_earnings_fact','key_columns' : ['reporting_id']},
                     {   'dim_table_name' :'tickers_eps_fact','key_columns' : ['id']},
                     {   'dim_table_name' :'tickers_financials_fact','key_columns' : ['reporting_id']},
                     {   'dim_table_name' :'tickers_historical_prices_fact','key_columns' : ['ticker','date','datetime','interval']},
                     {   'dim_table_name' :'tickers_holders_fact','key_columns' : ['ticker','holder']},
                     {   'dim_table_name' :'tickers_info_dim','key_columns' : ['ticker']}, #AOS,MMM
                     {   'dim_table_name' :'tickers_options_fact','key_columns' : ['contract_symbol']},
                     {   'dim_table_name' :'tickers_recommendations_fact', 'key_columns' :['ticker','firm','date']},
                     {   'dim_table_name' :'exchanges_dim', 'key_columns' :['exchange']},
                     {   'dim_table_name' :'markets_dim', 'key_columns' :['market']},
                     {   'dim_table_name' :'market_indexes_dim', 'key_columns' :['index_id','market']},
                     {   'dim_table_name' :'tickers_shares_fact','key_columns' : ['reporting_id']},
                     {   'dim_table_name' :'tickers_tweets_users_fact','key_columns' : ['ticker','user_id','tweet_id']},
                     {   'dim_table_name' :'tweets_dim','key_columns' : ['tweet_id']},
                     {   'dim_table_name' :'user_id_dim','key_columns' : ['user_id']}
]

def check_duplicates_and_count(dim_table_name, columns_list, spark,DL_BUCKET_OUTPUT_PREFIX):
    """
    The function returns the count of entries and the count of duplicates from the parquet files
    fetched from datalake.
    :param dim_table_name: The name of the table in the data model.
    :param columns_list: the list of columns considered as primary keys in the table.
    :param spark: spark object
    :param DL_BUCKET_OUTPUT_PREFIX: the prefix of the input folder
    :return: count of entries and count of duplicates
    """
    columns_list = [col.strip() for col in columns_list]
    path = os.path.join(DL_BUCKET_OUTPUT_PREFIX,f'{dim_table_name}/{dim_table_name}.parquet').replace('s3','s3a')
    dim_table =spark.read.parquet(os.path.join(path))
    print('\ntable :', dim_table_name)
    print('primary keys :', columns_list)
    dim_table_duplicates = dim_table.groupBy(columns_list).count().filter(col('count') >1 )
    print("count :" , dim_table.count(),'; duplicates : ' ,dim_table_duplicates.count())
    return dim_table.count(),dim_table_duplicates.count()



def main():

    global dim_table_name_ls
    print("create spark session")
    spark = create_spark_session()

    config = configparser.ConfigParser()
    config.read('/home/hadoop/dl_config.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    DL_BUCKET_OUTPUT_PREFIX = config.get('S3_BUCKET', 'DL_BUCKET_OUTPUT_PREFIX')

    checks_df = pd.DataFrame([], columns=['dimensional_table', 'key_columns', 'count', 'duplicates'])
    for table in dim_table_name_ls:
        count, duplicates = check_duplicates_and_count(table['dim_table_name'],\
                                                       table['key_columns'], spark,DL_BUCKET_OUTPUT_PREFIX)
        checks_df_tmp = pd.DataFrame([{'dimensional_table': table['dim_table_name'], \
                                       'key_columns': table['key_columns'], \
                                       'count : ': count, 'duplicates : ': duplicates}])
        checks_df = pd.concat([checks_df, checks_df_tmp])
    print(checks_df)
    if len(checks_df.query("(count == 0)  or (count.isnull() == 1)")) != 0:
        raise AssertionError('at least one table has no entries')
    elif len(checks_df.query("(duplicates > 0) and (count != 0) and (count.isnull() != 1)")) != 0:
        print(duplicates.show(10))
        raise AssertionError('at least one table has duplicates')
    else:
        print('tables correctly fed')

if __name__ == "__main__":
    main()