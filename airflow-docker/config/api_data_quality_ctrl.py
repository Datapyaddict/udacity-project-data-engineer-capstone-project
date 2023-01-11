
import pandas as pd
import os
import configparser
from common_modules import create_spark_session
from pyspark.sql.functions import expr, col

parquet_path_ls = [
                "markets_static_data/tickers_historical_prices/tickers_historical_prices.parquet",
                "markets_static_data/tickers_info/tickers_info.parquet",
                "markets_static_data/tickers_isin/tickers_isin.parquet",
                "markets_static_data/tickers_options/tickers_options.parquet",
                "markets_static_data/market_indexes/markets_indexes.parquet",
                "markets_static_data/tickers_actions/tickers_actions.parquet",
                "markets_static_data/tickers_analysis/tickers_analysis.parquet",
                "markets_static_data/tickers_balancesheet/tickers_balancesheet.parquet",
                "markets_static_data/tickers_cashflow/tickers_cashflow.parquet",
                "markets_static_data/tickers_earnings/tickers_earnings.parquet",
                "markets_static_data/tickers_eps/tickers_eps.parquet",
                "markets_static_data/tickers_financials/tickers_financials.parquet",
                "markets_static_data/tickers_holders/tickers_holders.parquet",
                "markets_static_data/tickers_major_holders/tickers_major_holders.parquet",
                "markets_static_data/tickers_news/tickers_news.parquet",
                "markets_static_data/tickers_recommendations/tickers_recommendations.parquet",
                "markets_static_data/tickers_shares/tickers_shares.parquet"
               ]
json_path_ls = ["tweets",]

def check_count(folder_path, spark,DL_BUCKET_INPUT_PREFIX, file_type):
    """
    This function returns a count of entries fetched from parquet/json files in input folder.
    :param folder_path:path to the parquet/json files to check data quality
    :param spark: spark connection object
    :param DL_BUCKET_INPUT_PREFIX: s3 path partial path fetched from dl_config,cfg
    :param file_type: 'parquet' or 'json'
    :return:the count of entries in the json or parquet file
    """
    path = os.path.join(DL_BUCKET_INPUT_PREFIX, folder_path).replace('s3', 's3a')
    if file_type == 'parquet':
        data =spark.read.parquet(path)
    if file_type == 'json':
        data =spark.read.json(path)
    print("folder path : ", folder_path, "; count :" , data.count())
    return data.count()


def main():

    global path_ls
    print("create spark session")
    spark = create_spark_session()

    config = configparser.ConfigParser()
    config.read('/home/hadoop/dl_config.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    DL_BUCKET_INPUT_PREFIX = config.get('S3_BUCKET', 'DL_BUCKET_INPUT_PREFIX')

    checks_df = pd.DataFrame([], columns=['folder_path', 'count'])
    for i in range(len(parquet_path_ls)):
        count = check_count(parquet_path_ls[i], spark, DL_BUCKET_INPUT_PREFIX, 'parquet')
        checks_df_tmp = pd.DataFrame([{'folder_path': parquet_path_ls[i], 'count': count}])
        checks_df = pd.concat([checks_df, checks_df_tmp])

    for i in range(len(json_path_ls)):
        count = check_count(json_path_ls[i], spark, DL_BUCKET_INPUT_PREFIX, 'json')
        checks_df_tmp = pd.DataFrame([{'folder_path': json_path_ls[i], 'count': count}])
        checks_df = pd.concat([checks_df, checks_df_tmp])

    print(checks_df)
    if len(checks_df.query("(count == 0) or (count.isnull() == 1)")) != 0:
        raise AssertionError('at least one table has no entries')
    else:
        print('main input data correctly fed from api')

if __name__ == "__main__":
    main()