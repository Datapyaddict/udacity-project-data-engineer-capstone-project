
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , DoubleType ,\
                                StringType , IntegerType , DateType ,\
                                TimestampType,DecimalType,LongType

from pyspark.sql.functions import col


import configparser
import yfinance as yf
import boto3
import json
import time

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def create_spark_session():
    """
    The function creates a spark session and returns a spark session object.
    """
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.2.2")\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider',\
            'com.amazonaws.auth.DefaultAWSCredentialsProviderChain') \
    .getOrCreate()
    return spark


    
def get_tickers_ls(num_tickers,constituents_csv_path,spark):
    """
    The function reads in the file "constituents.csv" which contains all the tickers from SP500.
    It will pick up only the first n tickers entered in parameter num_tickers from the list and return a list of them and a string
    with the tickers concatenated and separated by a blank character.
    :param num_tickers: the number of tickers we want to work on. It is set to 2 by default in dl_config.cfg file.
    :param constituents_csv_path: path to the constituents.csv file which contains all the tickers from SP500.
    :param spark:spark session object.
    :return: the list of tickers and a string containing tickers separated by a blank character.
    """

    sp500_companies = spark.read.format("csv").option("header","true").load(constituents_csv_path).toPandas()
    print(sp500_companies.head(2))
    tickers_ls = [ticker for ticker in sp500_companies['Symbol']]
    tickers_ls = tickers_ls[:num_tickers]
    
    multiple_tickers = ' '.join(tickers_ls)
    return tickers_ls, multiple_tickers

def get_tickers_object(multiple_tickers):
    """
    Based on a string of concactenated tickers , the function returns an object of all the tickers info.
    :param multiple_tickers: a string containing tickers separated by a blank character
    :return: an object of all the tickers info.
    """

    tickers = yf.Tickers(multiple_tickers)
    return tickers


def aws_client(s3_service, region, access_key_id, secret_access_key):
    """
    Create client object to access AWS s3 bucket
    :param s3_service: expected value is 'S3'
    :param region: the region parameter as is determined in dl_config.cfg file
    :param access_key_id: AWS access key id
    :param secret_access_key: AWS secret access key
    :return: s3 client object
    """

    client = boto3.client(s3_service,\
                          region_name=region,\
                          aws_access_key_id=access_key_id,\
                          aws_secret_access_key=secret_access_key
                          )
    return client

def create_s3_client(aws_access_key_id,aws_secret_access_key):
    """
    Create client object to access AWS s3 bucket.
    boto3.Session is larger than boto3.client.
    :param access_key_id: AWS access key id
    :param secret_access_key: AWS secret access key
    :return: s3 client object
    """
    session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
    )

    s3_client = session.resource('s3')
    return s3_client

def copy_and_delete_s3_keys(s3_client,dl_bucket_name,dl_bucket_output_prefix):
    """
    The existing folder (dl_bucket_output_prefix) containing parquet files
    is renamed from *.parquet/ to *_tmp.parquet/.
    :param s3_client: s3 client object
    :param dl_bucket_name: bucket name
    :param dl_bucket_output_prefix: prefix of the bucket folder in datalake
    :return: None
    """

    print('rename output folder from *.parquet to *_tmp.parquet')
    condition = True
    try:
        while condition:
            objects = s3_client.list_objects(Bucket= dl_bucket_name, Prefix=dl_bucket_output_prefix)
            print(json.dumps(objects, indent=4, sort_keys=True, default=str))

            content = objects.get('Contents', [])
            if len(content) == 0:
#                 break
                print('no keys in ', dl_bucket_output_prefix)
                condition = False
            else:
                for obj in content:
                    print('copy and delete object ' , obj)
                    Key_old=obj['Key']
                    Key_new=obj['Key'].replace('.parquet/','_tmp.parquet/')
                    copy_source = {'Bucket': dl_bucket_name, 'Key': Key_old}
                    s3_client.copy_object(CopySource = copy_source, Bucket = dl_bucket_name, Key = Key_new)
                    s3_client.delete_object(Bucket = dl_bucket_name, Key = Key_old)
                time.sleep(20)

    except Exception as e:
        print(e)
    print(dl_bucket_output_prefix, " / old files are copied to tmp files and then deleted")


def delete_s3_keys(s3_client, dl_bucket_name, dl_bucket_output_prefix):
    """
    This function deletes keys pertaining to the output folder ending with *_tmp.parquet/.
    :param s3_client: s3 client object.
    :param dl_bucket_name: bucket name.
    :param dl_bucket_output_prefix: the output folder path containing the parquet files.
    :return: None
    """
    condition = True
    try:
        while condition:
            objects = s3_client.list_objects(Bucket=dl_bucket_name, Prefix=dl_bucket_output_prefix)
            print(json.dumps(objects, indent=4, sort_keys=True, default=str))

            content = objects.get('Contents', [])
            if len(content) == 0:
                #                 break
                condition = False
            else:
                for obj in content:
                    print('delete object ', obj)
                    s3_client.delete_object(Bucket='atn-dl-data', Key=obj['Key'])
                time.sleep(20)
    #         print("Delete s3 bucket")
    #         s3.delete_bucket(Bucket=OUTPUT_BUCKET_NAME)
    except Exception as e:
        print(e)
    print(dl_bucket_output_prefix, " is deleted")


def refresh_parquet_files(write_files, read_files, tmp_df, spark, s3_client, dl_bucket_name, dl_bucket_output_prefix,
                          columns_list):
    """
    The function updates the datalake parquet files with the latest data loaded from input folder.
    The update is done by combining the existing parquet files with the new data read in from input folder.
    Based on a list of keys from parameter column_list, we ensure there is no duplicate upon writing
    of the target files.
    :param write_files: the output folder path ending with *.parquet/.
    :param read_files: the output folder path ending with *_tmp.parquet/.
    :param tmp_df: dataframe containing the data fetched from parquet files in input folder.
    :param spark: spark object.
    :param s3_client: s3 client object.
    :param dl_bucket_name: bucket name.
    :param dl_bucket_output_prefix: the output folder path ending with *.parquet/.
    :param columns_list: list of column keys to ensure there is no duplicate
    :return: None
    """

    print('run copy_and_delete_s3_keys with ', dl_bucket_output_prefix)
    copy_and_delete_s3_keys(s3_client=s3_client, dl_bucket_name=dl_bucket_name, \
                            dl_bucket_output_prefix=dl_bucket_output_prefix)

    dl_bucket_output_prefix_tmp = dl_bucket_output_prefix.replace('.parquet/', '_tmp.parquet/')

    print(f'list keys in folder {dl_bucket_output_prefix_tmp} \n',
          s3_client.list_objects(Bucket=dl_bucket_name, Prefix=dl_bucket_output_prefix_tmp))

    tmp_files = s3_client.list_objects(Bucket=dl_bucket_name, \
                                       Prefix=dl_bucket_output_prefix_tmp)
    # print(tmp_files)
    if tmp_files.get('Contents'):
        print('tmp keys exist')
        old_df = spark.read.parquet(read_files)
        print('existing df :', old_df.count())
        new_df = old_df.union(tmp_df).dropDuplicates()
    else:
        new_df = tmp_df
    print('df/ count before : ', tmp_df.count())
    print('df/ count after : ', new_df.count())

    # new_df = keep_last_values(dim_table_name, spark, dl_bucket_output_prefix_0, columns_list)
    new_df = keep_last_values(new_df,  columns_list)


    print("write updated dimensional df in ", dl_bucket_output_prefix)
    new_df.write.mode('overwrite').parquet(write_files)

    time.sleep(20)

    print("Delete copied files in tmp folder \; ", dl_bucket_output_prefix_tmp)
    delete_s3_keys(s3_client=s3_client, dl_bucket_name=dl_bucket_name, \
                   dl_bucket_output_prefix=dl_bucket_output_prefix_tmp)

# def keep_last_values(dim_table_name, spark, dl_bucket_output_prefix_0, columns_list):
def keep_last_values(new_df, columns_list):
    """
    The function keeps the last entry in case there are more than one entry per partition keys in column_list.
    This ensures we have always a unique entry per partition keys.
    :param new_df: the new dataframe to be written into parquet files.
    :param columns_list: the list of partition keys.
    :return: None.
    """
    print('we keep the last entry per partition keys')
    print('new_df/ count before : ', new_df.count())

    window_spec = Window.partitionBy(columns_list).orderBy(columns_list)
    new_df = new_df.withColumn("row_number", row_number().over(window_spec))

    window_spec_order = Window.partitionBy(columns_list).orderBy(col('row_number').desc())
    new_df = new_df.withColumn("row_number_2", row_number().over(window_spec_order))
    new_df = new_df.filter(col("row_number_2") == 1).drop("row_number_2"). \
        drop("row_number")

    print('new_df/ count after : ', new_df.count())

    return new_df
