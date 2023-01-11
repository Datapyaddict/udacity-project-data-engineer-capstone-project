
# import pandas as pd
import os
import sys
import configparser
import tweepy as tw
# from pyspark.sql import SparkSession
import json
import boto3

from common_modules import create_spark_session
from common_modules import get_tickers_ls

from common_modules import create_s3_client


def create_api_session(config):
    """
    The function creates a Twitter API object based on the API credentials stored in the dl_config.cfg
    file.
    :param config: config parser object
    :return: API object
    """
    twitter_api_key = config.get('APIS', 'TWITTER_API_KEY')
    twitter_api_key_secret = config.get('APIS', 'TWITTER_API_KEY_SECRET')

    auth = tw.OAuthHandler(twitter_api_key, twitter_api_key_secret)
#     api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True )
    api = tw.API(auth, wait_on_rate_limit=True)

    return api

# def create_s3_client(aws_access_key_id,aws_secret_access_key):
#     """
#     """
#     session = boto3.Session(
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key
#     )
#
#     s3_client = session.resource('s3')
#     return s3_client

def get_write_tweets(tickers_ls,api,dl_bucket_name ,dl_bucket_input_folder,s3_client):
    """
    The function gets tweets based on the list of tickers and dump json files per ticker into the input folder.
    :param tickers_ls: list of tickers.
    :param api: Twitter API object.
    :param dl_bucket_name: bucket name.
    :param dl_bucket_input_folder: input folder name.
    :param s3_client: s3 client object.
    :return:
    """
    tweets_path = os.path.join(dl_bucket_input_folder,'tweets/')
    print('load tweets')
    for i in range(len(tickers_ls)):
        print(tickers_ls[i])
        symbol = tickers_ls[i]
        search_query = f"#{symbol} -filter:retweets'"

        try :
            tweets = tw.Cursor(api.search_tweets,
                      q=search_query,tweet_mode='extended',
                      lang="en").items(1000000)
        except tw.errors.TweepyException as e:
            print(e)
            continue

    # store the API responses in a list

        tweet_ls = []
        for tweet in tweets:
            tweet_info = {}
            tweet_info['ticker'] = symbol
            tweet_info['tweet_id'] = tweet.id
            tweet_info['created_at'] = tweet.created_at.strftime("%Y-%m-%d %H:%M:%S")
            tweet_info['in_reply_to_status_id'] = tweet.in_reply_to_status_id
            tweet_info['truncated'] = tweet.truncated
            text = tweet.full_text
            hashtags = tweet.entities['hashtags']

            hashtags_ls = []
            try:
                for hashtag in hashtags:
                    hashtags_ls.append(hashtag["text"])    
            except:
                pass
            tweet_info['hashtags'] = hashtags_ls
            tweet_info['text'] = text

            tweet_info['lang'] = tweet.metadata['iso_language_code']
            tweet_info['retweet_count']=tweet.retweet_count

            tweet_info['source']=tweet.source  
            tweet_info['source_url']=tweet.source_url 
            tweet_info['user_id']=tweet.user.id
            tweet_info['user_name']=tweet.user.name
            tweet_info['user_location']=tweet.user.location
            tweet_info['user_description']=tweet.user.description
            tweet_info['user_verified']=tweet.user._json['verified']
            tweet_info['user_followers_count']=tweet.user.followers_count
            tweet_info['friends_count']=tweet.user.friends_count
            tweet_info['user_created_at']=tweet.user.created_at.strftime('%Y-%m-%d')

            tweet_ls.append(tweet_info)
        try:
            target_dump_file = os.path.join(tweets_path,f'ticker_{symbol}.json')
            local_dump_file = f'ticker_{symbol}.json'

            with open(local_dump_file,"a") as json_file:
                json.dump(tweet_ls,json_file,indent=4,sort_keys=True)
                                
        except Exception as e:
            print(e)
            continue
            
        try:
            print('sending to s3')
            
            bucket = s3_client.Bucket(dl_bucket_name)

            with open(local_dump_file, 'rb') as data:
                print(data)
                result = bucket.upload_fileobj(data, target_dump_file)
                print(result)
        except Exception as e:
            print(e)
            continue
    
def main():
    
    config = configparser.ConfigParser()    
#     config.read('dl_config.cfg') 
    config.read('/home/hadoop/dl_config.cfg') 

    aws_access_key_id = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    region = config.get('CLUSTER', 'AWS_REGION')

    # DL_BUCKET_INPUT_PREFIX = config.get('S3_BUCKET','DL_BUCKET_INPUT_PREFIX')
    DL_BUCKET_INPUT_FOLDER = config.get('S3_BUCKET','DL_BUCKET_INPUT_FOLDER')

    DL_BUCKET_NAME = config.get('S3_BUCKET','DL_BUCKET_NAME')
    DL_BUCKET_SCRIPTS_FOLDER = config.get('S3_BUCKET','DL_BUCKET_SCRIPTS_FOLDER')
    num_tickers = int(config.get('APIS','TOP_N_TICKERS'))

    print('python version : ', sys.version)

    print("create spark session")   
    spark = create_spark_session()
    
    print("create api session")   
    api = create_api_session(config)
    
    print('create s3 client')
    s3_client = create_s3_client(aws_access_key_id,aws_secret_access_key)

    constituents_csv_path = os.path.join('s3://',DL_BUCKET_NAME,DL_BUCKET_SCRIPTS_FOLDER,'constituents.csv')
    print(constituents_csv_path)

    print(f"get {num_tickers} tickers list")
    tickers_ls, multiple_tickers = get_tickers_ls(num_tickers,constituents_csv_path,spark)
    
    print('get and write tweets in json files')       
    get_write_tweets(tickers_ls,api,DL_BUCKET_NAME,DL_BUCKET_INPUT_FOLDER,s3_client)




if __name__ == "__main__":
    main()
