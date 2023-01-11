### Overview od scripts

#### list of Airflow DAGs

All DAGs are run locally in a docker container.

|script name| DAG name/ Custom operator| location|
|-|-|-|
|import_data_from_apis.py|DAG1|airflow-docker/dags/market_data_analytics/common_packages/|
|datalake_update.py|DAG2|airflow-docker/dags/market_data_analytics/my_dags/|
|empty_data_lake.py|DAG3|airflow-docker/dags/market_data_analytics/my_dags/|
|empty_s3_folder.py|custom operator|airflow-docker/dags/market_data_analytics/common_packages/|

`empty_s3_folder.py` is a __custom operator__ which is called by DAG1 to empty the folder `input` and DAG3 to empty the folder `datalake`.

#### list of scripts

All scripts are run in an EMR instance.

|script name|input source |output files location|
|-|-|-|
|[get_stocksymbol_data.py](#get_stocksymbol_data.py)|stock symbol API|input/markets_static_data/tickers_exchanges/tickers_exchanges_[market].parquet/|
|[common_modules.py](#common_modules.py)|None |None|
|[get_tickers_actions.py](#get_tickers_actions.py)|yahoo finance API |input/markets_static_data/tickers_actions/tickers_actions.parquet/|
|[get_tickers_analysis.py](#get_tickers_analysis.py)|yahoo finance API |input/markets_static_data/tickers_analysis/tickers_analysis.parquet/|
|[get_tickers_balancesheet.py](#get_tickers_balancesheet.py)|yahoo finance API |input/markets_static_data/tickers_balancesheet/tickers_balancesheet.parquet/|
|[get_tickers_cashflow.py](#get_tickers_cashflow.py)|yahoo finance API |input/markets_static_data/tickers_cashflow/tickers_cashflow.parquet/|
|[get_tickers_earnings.py](#get_tickers_earnings.py)|yahoo finance API |input/markets_static_data/tickers_earnings/tickers_earnings.parquet/|
|[get_tickers_earnings_dates.py](#get_tickers_earnings_dates.py)|yahoo finance API |input/markets_static_data/tickers_eps/tickers_eps.parquet/|
|[get_tickers_financials.py](#get_tickers_financials.py)|yahoo finance API |input/markets_static_data/tickers_financials/tickers_financials.parquet/|
|[get_tickers_historical_prices.py](#get_tickers_historical_prices.py)|yahoo finance API |input/markets_static_data/tickers_historical_prices/tickers_historical_prices.parquet/|
|[get_tickers_holders.py](#get_tickers_holders.py)|yahoo finance API |input/markets_static_data/tickers_holders/tickers_holders.parquet/|
|[get_tickers_info.py](#get_tickers_info.py)|yahoo finance API |input/markets_static_data/tickers_info/tickers_info.parquet/|
|[get_tickers_isin.py](#get_tickers_isin.py)|yahoo finance API |input/markets_static_data/tickers_isin/tickers_isin.parquet/|
|[get_tickers_major_holders.py](#get_tickers_major_holders.py)|yahoo finance API |input/markets_static_data/tickers_major_holders/tickers__major_holders.parquet/|
|[get_tickers_news.py](#get_tickers_news.py)|yahoo finance API |input/markets_static_data/tickers_news/tickers_news.parquet/|
|[get_tickers_options.py](#get_tickers_options.py)|yahoo finance API |input/markets_static_data/tickers_options/tickers_options.parquet/|
|[get_tickers_recommendations.py](#get_tickers_recommendations.py)|yahoo finance API |input/markets_static_data/tickers_recommendations/tickers_recommendations.parquet/|
|[get_tickers_shares.py](#get_tickers_shares.py)|yahoo finance API |input/markets_static_data/tickers_shares/tickers_shares.parquet/|
|[get_tickers_tweets.py](#get_tickers_tweets.py)|yahoo finance API |input/tweets/ticker_[ticker].json/|
|[put_tickers_actions_dim.py](#put_tickers_actions_dim.py)|input/markets_static_data/tickers_actions/tickers_actions.parquet/|datalake/tickers_actions_fact/tickers_actions_fact.parquet/|
|[put_exchanges_dim.py](#put_exchanges_dim.py)|input/markets_static_data/tickers_exchanges/tickers_exchanges_[market].parquet/|datalake/exchanges_dim/exchanges_dim.parquet/|
|[put_market_dim.py](#put_market_dim.py)|input/markets_static_data/tickers_exchanges/tickers_exchanges_[market].parquet/|datalake/markets_dim/markets_dim.parquet/, datalake/market_indexes_dim/market_indexes.parquet/|
|[put_tickers_analysis_dim.py](#put_tickers_analysis_dim.py)|input/markets_static_data/tickers_analysis/tickers_analysis.parquet/|datalake/tickers_analysis_fact/tickers_analysis_fact.parquet/|
|[put_tickers_balancesheet_dim.py](#put_tickers_balancesheet_dim.py)|input/markets_static_data/tickers_balancesheet/tickers_balancesheet.parquet/|datalake/financial_report_dim/financial_report_dim.parquet/, datalake/tickers_balancesheet_fact/tickers_balancesheet_fact.parquet/|
|[put_tickers_cashflow_dim.py](#put_tickers_cashflow_dim.py)|input/markets_static_data/tickers_cashflow/tickers_cashflow.parquet/|datalake/financial_report_dim/financial_report_dim.parquet/, datalake/tickers_cashflow_fact/tickers_cashflow_fact.parquet/|
|[put_tickers_earnings_dim.py](#put_tickers_earnings_dim.py)|input/markets_static_data/tickers_earnings/tickers_earnings.parquet/|datalake/financial_report_dim/financial_report_dim.parquet/, datalake/tickers_earnings_fact/tickers_earnings_fact.parquet/|
|[put_tickers_eps_dim.py](#put_tickers_eps_dim.py)|input/markets_static_data/tickers_eps/tickers_eps.parquet/|datalake/tickers_eps_fact/tickers_eps_fact.parquet/|
|[put_tickers_financials_dim.py](#put_tickers_financials_dim.py)|input/markets_static_data/tickers_financials/tickers_financials.parquet/|datalake/financial_report_dim/financial_report_dim.parquet/, datalake/tickers_financials_fact/tickers_financials_fact.parquet/|
|[put_tickers_historical_prices_dim.py](#put_tickers_historical_prices_dim.py)|input/markets_static_data/tickers_historical_prices/tickers_historical_prices.parquet/|datalake/tickers_historical_prices_fact/tickers_historical_prices_fact.parquet/|
|[put_tickers_holders_dim.py](#put_tickers_holders_dim.py)|input/markets_static_data/tickers__major_holders/tickers_major_holders.parquet/|datalake/holders_dim/holders_dim.parquet/, datalake/tickers_holders_fact/tickers_holders_fact.parquet/|
|[put_tickers_info_dim.py](#put_tickers_info_dim.py)|input/markets_static_data/tickers_info/tickers_info.parquet/|datalake/location_dim/location_dim.parquet/, datalake/sector_dim/sector_dim.parquet/, datalake/tickers_info_dim/tickers_info_dim.parquet/|
|[put_tickers_news_dim.py](#put_tickers_news_dim.py)|input/markets_static_data/tickers_news/tickers_news.parquet/|datalake/news_dim/news_dim.parquet/, datalake/tickers_news_fact/tickers_news_fact.parquet/|
|[put_tickers_options_dim.py](#put_tickers_options_dim.py)|input/markets_static_data/tickers_news/tickers_news.parquet/|datalake/tickers_options_fact/tickers_options_fact.parquet/|
|[put_tickers_recommendations_dim.py](#put_tickers_recommendations_dim.py)|input/markets_static_data/tickers_recommendations/tickers_recommendations.parquet/|datalake/tickers_recommendations_fact/tickers_recommendations_fact.parquet/|
|[put_tickers_shares_dim.py](#put_tickers_shares_dim.py)|input/markets_static_data/tickers_shares/tickers_shares.parquet/|datalake/financial_report_dim/financial_report_dim.parquet/, datalake/tickers_shares_fact/tickers_shares_fact.parquet/|
|[put_tickers_tweets_dim.py](#put_tickers_tweets_dim.py)|input/tweets/ticker_[ticker].json/|datalake/tweets_dim/tweets_dim.parquet/, datalake/user_id_dim/user_id_dim.parquet/, datalake/hashtags_dim/hashtags_dim.parquet/, datalake/tickers_tweets_users_fact/tickers_tweets_users_fact.parquet/| 
|[api_data_quality_ctrl.py](#api_data_quality_ctrl.py)|input/|None|
|[datalake_quality_ctrl.py](#datalake_quality_ctrl.py)|datalake/|None|


<a id= 'get_stocksymbol_data.py'></a>
##### get_stocksymbol_data.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|This script writes the stock markets data from api into json files in s3 bucket|None|None|`get_stocksymbol_data.get_stocksymbol_data`|None|
|`get_stocksymbol_data`|This function calls the list of stock markets via api from the stocksymbol module and writes the data back into json files per market in the s3 bucket.|`spark`, `api_key`, `s3_bucket_prefix` ,`sparkContext`, `sqlContext`|`spark`: spark object, `api_key`: api key to use stocksymbol api, `s3_bucket_prefix`: the s3 bucket prefix that contains the files produced by the function, `sparkContext`: sparkContext object, `sqlContext`: sqlContext object|None|None|

<a id= 'common_modules.py'></a>
##### common_modules.py

Common_modules.py is a module containing functions called by other scripts.

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`get_tickers_ls`|The function reads in the file `constituents.csv` which contains all the tickers from SP500. It will pick up only the first n tickers entered in parameter num_tickers from the SP500 list and return the selected tickers as a list and as a string with the tickers concatenated and separated by a blank character|`num_tickers`,`constituents_csv_path`,`spark`|`num_tickers` : the number of tickers we want to work on. It is set to 2 by default in `dl_config.cfg` file. `constituents_csv_path`: path to the constituents.csv file which contains all the tickers from SP500. `spark`: spark session object.|None|the list of tickers and a string containing tickers separated by a blank character.|
|`get_tickers_object`|Based on a string of concactenated tickers , the function returns an object of all the tickers info.|`multiple_tickers`|`multiple_tickers`: a string containing tickers separated by a blank character|None|an object of all the tickers info|
|`aws_client`|Create client object to access AWS s3 bucket|`s3_service`, `region`, `access_key_id`,`secret_access_key`|`s3_service`: expected value is 'S3', `region`: the region parameter as is determined in `dl_config.cfg` file, `access_key_id`: AWS access key id, `secret_access_key`: AWS secret access key |None|s3 client object|
|`create_s3_client`|Create client object to access AWS s3 bucket. Boto3.Session is larger than boto3.client.|`aws_access_key_id`,`aws_secret_access_key`|`aws_access_key_id`: AWS access key id, `aws_secret_access_key`: AWS secret access key |None|s3 client object|
|`copy_and_delete_s3_keys`|The existing folder `dl_bucket_output_prefix` containing parquet files is renamed from `*.parquet/` to `*_tmp.parquet/.`|`s3_client`,`dl_bucket_name`,`dl_bucket_output_prefix`|`s3_client`: s3 client object, `dl_bucket_name`: bucket name, `dl_bucket_output_prefix`: prefix of the bucket folder in datalake|None|None|
|`delete_s3_keys`|This function deletes keys pertaining to the output folder ending with *_tmp.parquet/.|`s3_client`,`dl_bucket_name`,`dl_bucket_output_prefix`|`s3_client`: s3 client object, `dl_bucket_name`: bucket name, `dl_bucket_output_prefix`: the output folder path containing the parquet files|None|None|
|`refresh_parquet_files`|The function updates the datalake parquet files with the latest data loaded from input folder. The update is done by combining the existing parquet files with the new data read in from input folder. Based on a list of keys from parameter column_list, we ensure there is no duplicate upon writing of the target files.|`write_files`, `read_files`, `tmp_df`,`spark`, `s3_client`, `dl_bucket_name`,`dl_bucket_output_prefix`,`columns_list`|`write_files`: the output folder path ending with `*.parquet/`, `read_files`: the output folder path ending with `*_tmp.parquet/`, `tmp_df`: dataframe containing the data fetched from parquet files in input folder, `spark`: spark object, `s3_client`: s3 client object, `dl_bucket_name`: bucket name, `dl_bucket_output_prefix`: the output folder path ending with `*.parquet/`, `columns_list`: list of column keys to ensure there is no duplicate|`copy_and_delete_s3_keys`, `keep_last_values`, `delete_s3_keys`|None|
|`keep_last_values(new_df, columns_list)`|The function keeps the last entry in case there are more than one entry per partition keys in column_list. This ensures we have always a unique entry per partition keys.|`new_df`, `columns_list`|`new_df`: the new dataframe to be written into parquet files, `columns_list`: the list of partition keys.|None|None|

<a id= 'get_tickers_actions.py'></a>
##### get_tickers_actions.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves actions related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_actions.get_tickers_actions`, `get_tickers_actions.write_tickers_actions_to_parquet`|None|
|`get_tickers_actions`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers actions info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers actions|
|`write_tickers_actions_to_parquet`|The function creates a dataframe from the tickers actions info and writes the data into a parquet file in input folder.|`tickers_actions_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_actions_ls`: tickers actions list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_analysis.py'></a>
##### get_tickers_analysis.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves analysis related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_analysis.get_tickers_analysis`, `get_tickers_analysis.write_tickers_analysis_to_parquet`|None|
|`get_tickers_analysis`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers analysis info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers analyses|
|`write_tickers_analysis_to_parquet`|The function creates a dataframe from the tickers analysis info and writes the data into a parquet file in input folder.|`tickers_analysis_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_analysis_ls`: tickers analysis list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_balancesheet.py'></a>
##### get_tickers_balancesheet.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves balancesheet related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_balancesheet.get_tickers_balancesheet`, `get_tickers_balancesheet.write_tickers_balancesheet_to_parquet`|None|
|`get_tickers_balancesheet`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers balancesheet info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers balancesheet|
|`write_tickers_balancesheet_to_parquet`|The function creates a dataframe from the tickers balancesheet info and writes the data into a parquet file in input folder.|`tickers_balancesheet_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_balancesheet_ls`: tickers balancesheet list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_cashflow.py'></a>
##### get_tickers_cashflow.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves cashflow related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_cashflow.get_tickers_cashflow`, `get_tickers_cashflow.write_tickers_cashflow_to_parquet`|None|
|`get_tickers_cashflow`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers cashflow info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers cashflow|
|`write_tickers_cashflow_to_parquet`|The function creates a dataframe from the tickers cashflow info and writes the data into a parquet file in input folder.|`tickers_cashflow_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_cashflow_ls`: tickers cashflow list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_earnings.py'></a>
##### get_tickers_earnings.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves earnings related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_earnings.get_tickers_earnings`, `get_tickers_earnings.write_tickers_earnings_to_parquet`|None|
|`get_tickers_earnings`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers earnings info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers earnings|
|`write_tickers_earnings_to_parquet`|The function creates a dataframe from the tickers earnings info and writes the data into a parquet file in input folder.|`tickers_earnings_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_earnings_ls`: tickers earnings list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_earnings_dates.py'></a>
##### get_tickers_earnings_dates.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves earnings_dates related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_earnings_dates.get_tickers_earnings_dates`, `get_tickers_earnings_dates.write_tickers_earnings_dates_to_parquet`|None|
|`get_tickers_earnings_dates`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers earnings_dates info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers earnings_dates|
|`write_tickers_earnings_dates_to_parquet`|The function creates a dataframe from the tickers earnings_dates info and writes the data into a parquet file in input folder.|`tickers_earnings_dates_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_earnings_dates_ls`: tickers earnings_dates list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_financials.py'></a>
##### get_tickers_financials.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves financials related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_financials.get_tickers_financials`, `get_tickers_financials.write_tickers_financials_to_parquet`|None|
|`get_tickers_financials`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers financials info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers financials|
|`write_tickers_financials_to_parquet`|The function creates a dataframe from the tickers financials info and writes the data into a parquet file in input folder.|`tickers_financials_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_financials_ls`: tickers financials list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_historical_prices.py'></a>
##### get_tickers_historical_prices.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves financial prices data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `get_tickers_historical_prices.get_tickers_historical_prices`, `get_tickers_historical_prices.write_tickers_historical_prices_to_parquet`|None|
|`get_tickers_historical_prices`|Based on the list of tickers , the function returns a list of tickers historical prices based on the `interval` and the max `period`. If interval = 1m ( one minute), max period = 7 days. If interval = 1h ( one minute), max period = 2 years. If interval = 1d ( one minute), max period = 10 years.|`multiple_tickers`, `interval`, `period`)|`multiple_tickers`: string of tickers separated by a blank space, `interval`: interval parameter, `period`: period parameter|None|historical prices object|
|`write_tickers_historical_prices_to_parquet`|The function creates a dataframe from the tickers historical prices object and writes a parquet file.|`historical_prices`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`historical_prices`: historical prices object, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_holders.py'></a>
##### get_tickers_holders.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves holders related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_holders.get_tickers_holders`, `get_tickers_holders.write_tickers_holders_to_parquet`|None|
|`get_tickers_holders`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers holders info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers holders|
|`write_tickers_holders_to_parquet`|The function creates a dataframe from the tickers holders info and writes the data into a parquet file in input folder.|`tickers_holders_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_holders_ls`: tickers holders list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_info.py'></a>
###### get_tickers_info.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves info related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_isin.get_tickers_info`, `get_tickers_isin.write_tickers_info_to_parquet`|None|
|`get_tickers_info`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers info info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers info|
|`write_tickers_info_to_parquet`|The function creates a dataframe from the tickers info info and writes the data into a parquet file in input folder.|`tickers_info_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_info_ls`: tickers info list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_isin.py'></a>
##### get_tickers_isin.py
    
|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves isin related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_isin.get_tickers_isin`, `get_tickers_isin.write_tickers_isin_to_parquet`|None|
|`get_tickers_isin`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers isin info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers isin|
|`write_tickers_isin_to_parquet`|The function creates a dataframe from the tickers isin info and writes the data into a parquet file in input folder.|`tickers_isin_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_isin_ls`: tickers isin list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_major_holders.py'></a>
##### get_tickers_major_holders.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves major_holders related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_major_holders.get_tickers_major_holders`, `get_tickers_major_holders.write_tickers_major_holders_to_parquet`|None|
|`get_tickers_major_holders`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers major_holders info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers major_holders|
|`write_tickers_major_holders_to_parquet`|The function creates a dataframe from the tickers major_holders info and writes the data into a parquet file in input folder.|`tickers_major_holders_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_major_holders_ls`: tickers major_holders list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_news.py'></a>
##### get_tickers_news.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves news related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_news.get_tickers_news`, `get_tickers_news.write_tickers_news_to_parquet`|None|
|`get_tickers_news`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers news info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers news|
|`write_tickers_news_to_parquet`|The function creates a dataframe from the tickers news info and writes the data into a parquet file in input folder.|`tickers_news_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_news_ls`: tickers news list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_options.py'></a>
##### get_tickers_options.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves options related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_options.get_tickers_options`, `get_tickers_options.write_tickers_options_to_parquet`|None|
|`get_tickers_options`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers options info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers options|
|`write_tickers_options_to_parquet`|The function creates a dataframe from the tickers options info and writes the data into a parquet file in input folder.|`tickers_options_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_options_ls`: tickers options list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_recommendations.py'></a>
##### get_tickers_recommendations.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves recommendations related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_recommendations.get_tickers_recommendations`, `get_tickers_recommendations.write_tickers_recommendations_to_parquet`|None|
|`get_tickers_recommendations`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers recommendations info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers recommendations|
|`write_tickers_recommendations_to_parquet`|The function creates a dataframe from the tickers recommendations info and writes the data into a parquet file in input folder.|`tickers_recommendations_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_recommendations_ls`: tickers recommendations list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_shares.py'></a>
##### get_tickers_shares.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves shares related data for the selected tickers from api and writes data back into parquet files in input folder |None|None|`common_modules.create_spark_session`, `common_modules.get_tickers_ls`, `common_modules.get_tickers_object`, `get_tickers_shares.get_tickers_shares`, `get_tickers_shares.write_tickers_shares_to_parquet`|None|
|`get_tickers_shares`|Based on the list of tickers `tickers_ls` and tickers object `tickers`, the function returns a list of all the tickers shares info via an api request.|`tickers_ls`, `tickers`|`tickers_ls`: list of tickers, `tickers`: tickers object|None|the list of tickers shares|
|`write_tickers_shares_to_parquet`|The function creates a dataframe from the tickers shares info and writes the data into a parquet file in input folder.|`tickers_shares_ls`,`DL_BUCKET_INPUT_PREFIX`, `spark`|`tickers_shares_ls`: tickers shares list, `DL_BUCKET_INPUT_PREFIX`: prefix of the input folder, `spark`: spark object|None|None|

<a id= 'get_tickers_tweets.py'></a>
##### get_tickers_tweets.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the file `constituents.csv` and the number of tickers selected in `dl_confif.cfg` file, the script retrieves tweets related data for the selected tickers from api and writes data back into json files (per ticker) in input folder |None|None|`common_modules.create_s3_client`, `common_modules.get_tickers_ls`, `get_tickers_tweets.create_api_session`, `get_tickers_tweets.get_write_tweets`|None|
|`get_write_tweets`|The function creates a Twitter API object based on the API credentials stored in the `dl_config.cfg`|`config`|`config`: config parser object|None|API object|
|`get_write_tweets`|The function gets tweets based on the list of tickers and dump json files per ticker into the input folder.|`tickers_ls`,`api`,`dl_bucket_name` ,`dl_bucket_input_folder`,`s3_client`|`tickers_ls`: list of tickers, `api`: Twitter API object, `dl_bucket_name`: bucket name, `dl_bucket_input_folder`: input folder name, `s3_client`: s3 client object|None|None|

<a id= 'put_tickers_actions_dim.py'></a>
##### put_tickers_actions_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers actions related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_actions_dim.create_tmp_dimensional_tables_actions`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_actions`|This function creates a dataframe ( `tmp_tickers_actions_fact`) containing data from tickers actions fetched from input folder|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframe : `tmp_tickers_actions_fact`|

<a id= 'put_exchanges_dim.py'></a>
##### put_exchanges_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers exchanges related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_exchanges_dim.create_tmp_dimensional_tables_exchanges`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_exchanges`|This function creates a dataframe containing data from tickers actions fetched from input folder|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframe : `tmp_exchange_dim`|

<a id= 'put_market_dim.py'></a>
##### put_market_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers market indexes related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_market_dim.create_tmp_dimensional_tables_market_indexes`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_market_indexes`|This function creates 2 dataframe (`tmp_market_indexes_dim`, `tmp_market_dim`) from tickers market indexes fetched from input folder|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|2 dataframes : `tmp_market_indexes_dim`, `tmp_market_dim`|

<a id= 'put_tickers_analysis_dim.py'></a>
##### put_tickers_analysis_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers market analysis related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_analysis_dim.create_tmp_dimensional_tables_market_indexes`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_market_indexes`|This function creates a dataframe (`tmp_tickers_analysis_fact`) from tickers market analysis related data fetched from input folder|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframe : `tmp_tickers_analysis_fact`|

<a id= 'put_tickers_balancesheet_dim.py'></a>
##### put_tickers_balancesheet_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers market balansheet related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_balancesheet_dim.create_tmp_dimensional_tables_balancesheet`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_balancesheet`|This function creates 2 dataframes (`tmp_financial_report_dim`,`tmp_tickers_balancesheet_fact`) from tickers market balancesheet related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|2 dataframes : `tmp_financial_report_dim`, `tmp_tickers_balancesheet_fact`|

<a id= 'put_tickers_cashflow_dim.py'></a>
##### put_tickers_cashflow_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers cashflow related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_cashflow_dim.create_tmp_dimensional_tables_cashflow`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_cashflow`|This function creates 2 dataframes (`tmp_financial_report_dim`,`tmp_tickers_cashflow_fact`) from tickers cashflow related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|2 dataframes : `tmp_financial_report_dim`, `tmp_tickers_cashflow_fact`|

<a id= 'put_tickers_earnings_dim.py'></a>
##### put_tickers_earnings_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers earnings related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_earnings_dim.create_tmp_dimensional_tables_earnings`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_earnings`|This function creates 2 dataframes (`tmp_financial_report_dim`,`tmp_tickers_earnings_fact`) from tickers cashflow related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|2 dataframes : `tmp_financial_report_dim`, `tmp_tickers_earnings_fact`|

<a id= 'put_tickers_eps_dim.py'></a>
##### put_tickers_eps_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers eps related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_eps_dim.create_tmp_dimensional_tables_eps`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_eps`|This function creates a dataframe (`tmp_tickers_eps_fac`) from tickers eps related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframe : `tmp_tickers_eps_fact`|

<a id= 'put_tickers_financials_dim.py'></a>
##### put_tickers_financials_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers financials related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_financials_dim.create_tmp_dimensional_tables_financials`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_financials`|This function creates 2 dataframes (`tmp_financial_report_dim`,`tmp_tickers_financials_fact`) from tickers financials related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|2 dataframes : `tmp_financial_report_dim`, `tmp_tickers_financials_fact`|

<a id= 'put_tickers_historical_prices_dim.py'></a>
##### put_tickers_historical_prices_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers historical prices related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_historical_prices_dim.create_tmp_dimensional_tables_historical_prices`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_historical_prices`|This function creates 1 dataframe (`tmp_tickers_historical_prices_fact`) from tickers historical prices related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframe : `tmp_tickers_historical_prices_fact`|

<a id= 'put_tickers_holders_dim.py'></a>
##### put_tickers_holders_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers holders related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_holders_dim.create_tmp_dimensional_tables_holders`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_holders`|This function creates 2 dataframes (`tmp_holders_dim`,`tmp_tickers_holders_fact`) from tickers holders related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|2 dataframes : `tmp_financial_report_dim`, `tmp_tickers_holders_fact`|

<a id= 'put_tickers_info_dim.py'></a>
##### put_tickers_info_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers info related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_info_dim.create_tmp_dimensional_tables_info`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_info`|TThis function creates 3 dataframes (`tmp_location_dim, tmp_sector_dim`, `tmp_tickers_info_dim`) from tickers info related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|3 dataframes : `tmp_location_dim`, `tmp_sector_dim`, `tmp_tickers_info_dim`|

<a id= 'put_tickers_news_dim.py'></a>
##### put_tickers_news_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers news related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_news_dim.create_tmp_dimensional_tables_news`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_news`|This function creates 2 dataframes (`tmp_news_dim,tmp_tickers_news_fact`) from tickers news related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|2 dataframes : `tmp_news_dim` , `tmp_tickers_news_fact`|

<a id= 'put_tickers_options_dim.py'></a>
##### put_tickers_options_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers options related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_options_dim.create_tmp_dimensional_tables_options`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_options`|This function creates a dataframe (`tmp_tickers_options_fact`) from tickers options related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframe : `tmp_tickers_options_fact`|

<a id= 'put_tickers_recommendations_dim.py'></a>
##### put_tickers_recommendations_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers recommendations related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_recommendations_dim.create_tmp_dimensional_tables_recommendations`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_recommendations`|This function creates a dataframe (`tmp_tickers_recommendations_fact`) from tickers recommendations related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframe : `tmp_tickers_recommendations_fact`|

<a id= 'put_tickers_shares_dim.py'></a>
##### put_tickers_shares_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers shares related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_shares_dim.create_tmp_dimensional_tables_shares`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_shares`|This function creates 2 dataframes (`tmp_financial_report_dim`,`tmp_tickers_shares_fact`) from tickers shares related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframe : `tmp_financial_report_dim`,`tmp_tickers_shares_fact`|

<a id= 'put_tickers_tweets_dim.py'></a>
##### put_tickers_tweets_dim.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|Based on the tickers tweets related data stored in parquet files in input folder, the script retreats them to be compliant with the data model and dumps the target data into parquet files in the datalake folder. |None|None|`common_modules.create_spark_session`, `common_modules.aws_client`, `put_tickers_tweets_dim.create_tmp_dimensional_tables_tweets`, `common_modules.refresh_parquet_files`|None|
|`create_tmp_dimensional_tables_tweets`|This function creates 4 dataframes from tickers shares related data fetched from input folder.|`dl_bucket_input_prefix`, `spark`|`dl_bucket_input_prefix`: prefix of the input folder, `spark`: spark object |None|dataframes: `tmp_tweets_dim`, `tmp_user_id_dim`, `tmp_hashtags_dim`, `tmp_tickers_tweets_users_fact`|

<a id= 'api_data_quality_ctrl.py'></a>
##### api_data_quality_ctrl.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|This function checks there is at least one entry fetched from API. In case of fail, the scripts raises an assertion error.|None|None| `common_modules.create_spark_session`, `api_data_quality_ctrl.check_count`|None|
|`check_count`|This function returns a count of entries fetched from parquet/json files in input folder.|`folder_path`, `spark`, `DL_BUCKET_INPUT_PREFIX`, `file_type` |`folder_path`: path to the parquet files to check data quality, `spark`: spark connection object, `DL_BUCKET_INPUT_PREFIX`: s3 path partial path fetched from `dl_config.cfg`, `file_type`: 'parquet' or 'json' |None|count of entries|

<a id= 'datalake_quality_ctrl.py'></a>
##### datalake_quality_ctrl.py

|function|description|parameters|parameters description|functions called|return |
|-|-|-|-|-|-|
|`main`|This function checks there is at least one entry fetched from parquet files in datalake and also checks there is no duplicate. In case of fail, the scripts raises an assertion error.|None|None| `common_modules.create_spark_session`, `datalake_quality_ctrl.check_duplicates_and_count`|None|
|`check_duplicates_and_count`|The function returns the count of entries and the count of duplicates from the parquet files fetched from datalake.|`dim_table_name`, `columns_list`, `spark`, `DL_BUCKET_OUTPUT_PREFIX` |`dim_table_name`: The name of the table in the data model, `columns_list`: the list of columns considered as primary keys in the table, `spark`: spark object, `DL_BUCKET_OUTPUT_PREFIX`: the prefix of the input folder. |None|count of entries and count of duplicates|

####
