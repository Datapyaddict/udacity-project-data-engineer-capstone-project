from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from market_data_analytics.common_packages.empty_s3_folder import EmptyS3Folder

from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator

from airflow.operators.dagrun_operator import TriggerDagRunOperator


import configparser

# Get variables from config file
config = configparser.ConfigParser()
config_file_path = '/opt/airflow/config/dl_config.cfg'
config.read_file(open(config_file_path))

os.environ['AWS_DEFAULT_REGION'] = config.get('CLUSTER','AWS_REGION')

REGION                     = config.get('CLUSTER','AWS_REGION')
RELEASE_LABEL              = config.get("CLUSTER","EMR_RELEASE_LABEL")
EMR_INSTANCE_TYPE          = config.get("CLUSTER","EMR_INSTANCE_TYPE")
EMR_NUM_WORKERS            = int(config.get("CLUSTER","EMR_NUM_WORKERS"))

DL_BUCKET_NAME             = config.get('S3_BUCKET','DL_BUCKET_NAME')
DL_BUCKET_SCRIPTS_FOLDER  = config.get('S3_BUCKET','DL_BUCKET_SCRIPTS_FOLDER')
DL_BUCKET_LOGS_PREFIX      = config.get('S3_BUCKET','DL_BUCKET_LOGS_PREFIX')
DL_BUCKET_INPUT_FOLDER = config.get('S3_BUCKET','DL_BUCKET_INPUT_FOLDER')
DL_BUCKET_LOGS_FOLDER = config.get('S3_BUCKET','DL_BUCKET_LOGS_FOLDER')


INIT_STEPS = [
    {
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        'Name': 'copy scripts and config files to emr home',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://' + DL_BUCKET_NAME + '/' + DL_BUCKET_SCRIPTS_FOLDER, '/home/hadoop/', '--recursive']
        }
    },
    {
        'Name': 'install pip',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['pip3','install','--upgrade','pip'],
        }
    },
    {
        'Name': 'install configparser',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['pip3','install','--user','configparser']            
        
        }
    },
    {
    'Name': 'install yfinance',
    'ActionOnFailure': 'CANCEL_AND_WAIT',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['pip3','install','--user','yfinance']
    }
},   
    {
        'Name': 'install stocksymbol',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['pip3','install','--user','stocksymbol']
        }
    },     
    {
    'Name': 'install requests',
    'ActionOnFailure': 'CANCEL_AND_WAIT',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['pip3','install','--user','requests']
        }
    },  
    {
    'Name': 'install boto3',
    'ActionOnFailure': 'CANCEL_AND_WAIT',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['pip3','install','--user','boto3']
        }
    },   
    {
    'Name': 'install pandas',
    'ActionOnFailure': 'CANCEL_AND_WAIT',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['pip3','install','--user','pandas']
        }
    }, 
    {
    'Name': 'install tweepy',
    'ActionOnFailure': 'CANCEL_AND_WAIT',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['pip3','install','--user','tweepy']
        }
    },
]


STOCKSYMBOL_STEPS =  [
    {
        'Name': 'run get_stocksymbol script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_stocksymbol_data.py']
        }
    }              
]

GET_TICKERS_INFO_STEPS =  [

    {
        'Name': 'run get_tickers_info script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_info.py']
        }
    }               
]

GET_TICKERS_FINANCIALS_STEPS =  [

    {
        'Name': 'run get_tickers_financials script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_financials.py']
        }
    }            
]

GET_TICKERS_BALANCESHEET_STEPS =  [

    {
        'Name': 'run get_tickers_balancesheet script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_balancesheet.py']
        }
    }        
]

GET_TICKERS_CASHFLOW_STEPS =  [

    {
        'Name': 'run get_tickers_cashflow script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_cashflow.py']
        }
    }        
]

GET_TICKERS_EPS_STEPS =  [

    {
        'Name': 'run get_tickers_eps script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_earnings_dates.py']
        }
    }        
]

GET_TICKERS_EARNINGS_STEPS =  [

    {
        'Name': 'run get_tickers_earnings script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_earnings.py']
        }
    }        
]

GET_TICKERS_ACTIONS_STEPS =  [

    {
        'Name': 'run get_tickers_actions script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_actions.py']
        }
    }        
]

GET_TICKERS_MAJOR_HOLDERS_STEPS =  [

    {
        'Name': 'run get_major holders script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_major_holders.py']
        }
    }        
]

GET_TICKERS_HOLDERS_STEPS =  [

    {
        'Name': 'run get_tickers_holders script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_holders.py']
        }
    }        
]

GET_TICKERS_ANALYSIS_STEPS =  [

    {
        'Name': 'run get_tickers_analysis script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_analysis.py']
        }
    }        
]

GET_TICKERS_ISIN_STEPS =  [

    {
        'Name': 'run get_tickers_isin script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_isin.py']
        }
    }        
]

GET_TICKERS_NEWS_STEPS =  [

    {
        'Name': 'run get_tickers_news script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_news.py']
        }
    }        
]


GET_TICKERS_RECOMMENDATIONS_STEPS =  [

    {
        'Name': 'run get_tickers_recommendations script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_recommendations.py']
        }
    }        
]

GET_TICKERS_SHARES_STEPS =  [

    {
        'Name': 'run get_tickers_shares script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_shares.py']
        }
    }        
]

GET_TICKERS_OPTIONS_STEPS =  [

    {
        'Name': 'run get_tickers_options script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_options.py']
        }
    }        
]

GET_TICKERS_HISTORICAL_PRICES_STEPS =  [

    {
        'Name': 'run get_tickers_historical_prices script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_historical_prices.py']
        }
    }        
]

GET_TICKERS_TWEETS_STEPS =  [

    {
        'Name': 'run get_tickers_tweets script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/get_tickers_tweets.py']
        }
    }        
]

API_DATA_QUALITY_CTRL_STEPS =  [

    {
        'Name': 'run api_data_quality_control script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/api_data_quality_ctrl.py']
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'get-market-data-and-tweets',
    'ReleaseLabel': RELEASE_LABEL,
    'LogUri': DL_BUCKET_LOGS_PREFIX,
    'Applications':[
            {
                'Name': 'Spark'
            },
        ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'master_node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': EMR_INSTANCE_TYPE,
                'InstanceCount': 1,              
                
            },
            {
                'Name': "workers",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': EMR_INSTANCE_TYPE,
                'InstanceCount': EMR_NUM_WORKERS,
            }

        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}


dt = datetime.now()

default_args = {
    'owner': 'atn',
    'start_date': datetime(dt.year,dt.month,dt.day),
#     "depends_on_past": False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=1),
#     'catchup' : False,
#     'email_on_retry' : False,
}

parent_dag = DAG('atn_import_data_from_apis',
          default_args=default_args,
          description='tbd',
#           schedule_interval='0 * * * *',       
          schedule_interval='@once',
          tags=['pipeline'],
        )




start_operator = DummyOperator(task_id='Start_workflow',  dag=parent_dag)

# create bucket

bucket_creator = S3CreateBucketOperator(
    task_id='create_bucket',
    bucket_name=DL_BUCKET_NAME,
    region_name = REGION,
    aws_conn_id = "aws_credentials",
    dag = parent_dag,
)

#empty "input" folder
empty_input_folder_task = EmptyS3Folder(
    task_id='empty_input_folder',
    bucket_name=DL_BUCKET_NAME,
    folder = DL_BUCKET_INPUT_FOLDER,
    aws_conn_id = "aws_credentials",
    dag = parent_dag,
)

# empty logs folder
empty_logs_folder_task = EmptyS3Folder(
    task_id='empty_logs_folder',
    bucket_name=DL_BUCKET_NAME,
    folder = DL_BUCKET_LOGS_FOLDER,
    aws_conn_id = "aws_credentials",
    dag = parent_dag,
)

#upload files to bucket
common_modules_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_common_modules_py_to_s3",
        filename="/opt/airflow/config/common_modules.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'common_modules.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

init_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_init_py_to_s3",
        filename="/opt/airflow/config/__init__.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'__init__.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

api_data_quality_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_api_data_quality_py_to_s3",
        filename="/opt/airflow/config/api_data_quality_ctrl.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'api_data_quality_ctrl.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_stocksymbol_data_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_get_stocksymbol_data_py_to_s3",
        filename="/opt/airflow/config/get_stocksymbol_data.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_stocksymbol_data.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

dl_config_cfg_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_dl_config_cfg_to_s3",
        filename="/opt/airflow/config/dl_config.cfg",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'dl_config.cfg'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_info_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_get_yfinance_data_to_s3",
        filename="/opt/airflow/config/get_tickers_info.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_info.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

constituents_csv_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_constituents_csv_to_s3",
        filename="/opt/airflow/config/constituents.csv",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'constituents.csv'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_financials_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_financials_to_s3",
        filename="/opt/airflow/config/get_tickers_financials.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_financials.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_balancesheet_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_balancesheet_to_s3",
        filename="/opt/airflow/config/get_tickers_balancesheet.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_balancesheet.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_cashflow_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_cashflow_to_s3",
        filename="/opt/airflow/config/get_tickers_cashflow.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_cashflow.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_earnings_dates_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_eps_to_s3",
        filename="/opt/airflow/config/get_tickers_earnings_dates.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_earnings_dates.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_earnings_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_earnings_to_s3",
        filename="/opt/airflow/config/get_tickers_earnings.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_earnings.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_actions_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_actions_to_s3",
        filename="/opt/airflow/config/get_tickers_actions.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_actions.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_major_holders_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_major_holders_to_s3",
        filename="/opt/airflow/config/get_tickers_major_holders.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_major_holders.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_holders_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_holders_to_s3",
        filename="/opt/airflow/config/get_tickers_holders.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_holders.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_analysis_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_analysis_to_s3",
        filename="/opt/airflow/config/get_tickers_analysis.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_analysis.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_isin_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_isin_to_s3",
        filename="/opt/airflow/config/get_tickers_isin.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_isin.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_news_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_news_to_s3",
        filename="/opt/airflow/config/get_tickers_news.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_news.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_recommendations_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_recommendations_to_s3",
        filename="/opt/airflow/config/get_tickers_recommendations.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_recommendations.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_shares_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_shares_to_s3",
        filename="/opt/airflow/config/get_tickers_shares.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_shares.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_options_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_options_to_s3",
        filename="/opt/airflow/config/get_tickers_options.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_options.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_historical_prices_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_historical_prices_to_s3",
        filename="/opt/airflow/config/get_tickers_historical_prices.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_historical_prices.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

get_tickers_tweets_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_tickers_tweets_to_s3",
        filename="/opt/airflow/config/get_tickers_tweets.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'get_tickers_tweets.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = parent_dag
    )

# create EMR

emr_creator = EmrCreateJobFlowOperator(
        task_id='create_emr',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_credentials',
        emr_conn_id='aws_credentials',
        region_name = REGION,
        dag = parent_dag
    )

# add steps to emr
init_step_adder = EmrAddStepsOperator(
    task_id='add_init_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=INIT_STEPS,
    dag=parent_dag
)

stocksymbol_step_adder = EmrAddStepsOperator(
    task_id='add_stocksymbol_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=STOCKSYMBOL_STEPS,
    dag=parent_dag
)

#EmrStepSensor allows to check if the previous task has succeeded
stocksymbol_step_checker = EmrStepSensor(
        task_id='check_stocksymbol_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_stocksymbol_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_info_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_info_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_INFO_STEPS,
    dag=parent_dag
)

get_tickers_info_step_checker = EmrStepSensor(
        task_id='check_get_tickers_info_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_info_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_financials_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_financials_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_FINANCIALS_STEPS,
    dag=parent_dag
)

get_tickers_financials_step_checker = EmrStepSensor(
        task_id='check_get_tickers_financials_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_financials_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_balancesheet_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_balancesheet_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_BALANCESHEET_STEPS,
    dag=parent_dag
)

get_tickers_balancesheet_step_checker = EmrStepSensor(
        task_id='check_get_tickers_balancesheet_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_balancesheet_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )


get_tickers_cashflow_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_cashflow_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_CASHFLOW_STEPS,
    dag=parent_dag
)

get_tickers_cashflow_step_checker = EmrStepSensor(
        task_id='check_get_tickers_cashflow_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_cashflow_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_eps_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_eps_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_EPS_STEPS,
    dag=parent_dag
)

get_tickers_eps_step_checker = EmrStepSensor(
        task_id='check_get_tickers_eps_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_eps_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_earnings_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_earnings_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_EARNINGS_STEPS,
    dag=parent_dag
)

get_tickers_earnings_step_checker = EmrStepSensor(
        task_id='check_get_tickers_earnings_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_earnings_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_actions_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_actions_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_ACTIONS_STEPS,
    dag=parent_dag
)

get_tickers_actions_step_checker = EmrStepSensor(
        task_id='check_get_tickers_actions_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_actions_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_major_holders_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_major_holders_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_MAJOR_HOLDERS_STEPS,
    dag=parent_dag
)

get_tickers_major_holders_step_checker = EmrStepSensor(
        task_id='check_get_tickers_major_holders_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_major_holders_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_holders_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_holders_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_HOLDERS_STEPS,
    dag=parent_dag
)

get_tickers_holders_step_checker = EmrStepSensor(
        task_id='check_get_tickers_holders_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_holders_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_analysis_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_analysis_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_ANALYSIS_STEPS,
    dag=parent_dag
)

get_tickers_analysis_step_checker = EmrStepSensor(
        task_id='check_get_tickers_analysis_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_analysis_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_isin_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_isin_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_ISIN_STEPS,
    dag=parent_dag
)

get_tickers_isin_step_checker = EmrStepSensor(
        task_id='check_get_tickers_isin_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_isin_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_news_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_news_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_NEWS_STEPS,
    dag=parent_dag
)

get_tickers_news_step_checker = EmrStepSensor(
        task_id='check_get_tickers_news_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_news_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_recommendations_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_recommendations_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_RECOMMENDATIONS_STEPS,
    dag=parent_dag
)

get_tickers_recommendations_step_checker = EmrStepSensor(
        task_id='check_get_tickers_recommendations_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_recommendations_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_shares_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_shares_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_SHARES_STEPS,
    dag=parent_dag
)

get_tickers_shares_step_checker = EmrStepSensor(
        task_id='check_get_tickers_shares_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_shares_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_options_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_options_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_OPTIONS_STEPS,
    dag=parent_dag
)

get_tickers_options_step_checker = EmrStepSensor(
        task_id='check_get_tickers_options_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_options_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )

get_tickers_historical_prices_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_historical_prices_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_HISTORICAL_PRICES_STEPS,
    dag=parent_dag
)

get_tickers_historical_prices_step_checker = EmrStepSensor(
        task_id='check_get_tickers_historical_prices_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_historical_prices_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )


get_tickers_tweets_step_adder = EmrAddStepsOperator(
    task_id='add_get_tickers_tweets_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=GET_TICKERS_TWEETS_STEPS,
    dag=parent_dag
)

get_tickers_tweets_step_checker = EmrStepSensor(
        task_id='check_get_tickers_tweets_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_get_tickers_tweets_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )




cluster_end = EmrTerminateJobFlowOperator(
    task_id='end_emr_cluster',
    trigger_rule='all_done',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=parent_dag
)

api_data_quality_ctrl_step_adder = EmrAddStepsOperator(
    task_id='add_api_data_quality_ctrl_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=API_DATA_QUALITY_CTRL_STEPS,
    dag=parent_dag
)

api_data_quality_ctrl_step_checker = EmrStepSensor(
        task_id='check_api_data_quality_ctrl_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_api_data_quality_ctrl_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=parent_dag
    )


datalake_update_trigger = TriggerDagRunOperator(
    task_id="trigger_dag_atn_update_datalake",
    trigger_dag_id="atn_update_datalake",
    dag=parent_dag
)


end_operator = DummyOperator(task_id='Stop_workflow',  dag=parent_dag)



# bucket_deletion = S3DeleteBucketOperator(
#     task_id='delete_s3_bucket',
#     bucket_name=DL_BUCKET_NAME,
#     force_delete = True,
#     aws_conn_id = "aws_credentials",
#     dag = dag
# )


# tasks dependencies

start_operator >> bucket_creator
bucket_creator >> empty_input_folder_task
empty_input_folder_task >> empty_logs_folder_task
empty_logs_folder_task >> dl_config_cfg_to_s3_uploader
dl_config_cfg_to_s3_uploader >>common_modules_py_to_s3_uploader
common_modules_py_to_s3_uploader >> init_py_to_s3_uploader
init_py_to_s3_uploader >> get_stocksymbol_data_py_to_s3_uploader
get_stocksymbol_data_py_to_s3_uploader >> get_tickers_info_py_to_s3_uploader
get_tickers_info_py_to_s3_uploader >> constituents_csv_to_s3_uploader
constituents_csv_to_s3_uploader>> get_tickers_financials_py_to_s3_uploader
get_tickers_financials_py_to_s3_uploader >>get_tickers_balancesheet_py_to_s3_uploader
get_tickers_balancesheet_py_to_s3_uploader >> get_tickers_cashflow_py_to_s3_uploader
get_tickers_cashflow_py_to_s3_uploader >> get_tickers_earnings_dates_py_to_s3_uploader
get_tickers_earnings_dates_py_to_s3_uploader >> get_tickers_earnings_py_to_s3_uploader
get_tickers_earnings_py_to_s3_uploader >> get_tickers_actions_py_to_s3_uploader
get_tickers_actions_py_to_s3_uploader >> get_tickers_major_holders_py_to_s3_uploader
get_tickers_major_holders_py_to_s3_uploader >> get_tickers_holders_py_to_s3_uploader
get_tickers_holders_py_to_s3_uploader >> get_tickers_analysis_py_to_s3_uploader
get_tickers_analysis_py_to_s3_uploader >> get_tickers_isin_py_to_s3_uploader
get_tickers_isin_py_to_s3_uploader >> get_tickers_news_py_to_s3_uploader
get_tickers_news_py_to_s3_uploader >> get_tickers_recommendations_py_to_s3_uploader
get_tickers_recommendations_py_to_s3_uploader >> get_tickers_shares_py_to_s3_uploader
get_tickers_shares_py_to_s3_uploader >> get_tickers_options_py_to_s3_uploader
get_tickers_options_py_to_s3_uploader >> get_tickers_historical_prices_py_to_s3_uploader
get_tickers_historical_prices_py_to_s3_uploader >> get_tickers_tweets_py_to_s3_uploader
get_tickers_tweets_py_to_s3_uploader >> api_data_quality_py_to_s3_uploader
api_data_quality_py_to_s3_uploader >> emr_creator
emr_creator >> init_step_adder
init_step_adder >> stocksymbol_step_adder
stocksymbol_step_adder >> stocksymbol_step_checker
stocksymbol_step_checker >> get_tickers_info_step_adder
get_tickers_info_step_adder >> get_tickers_info_step_checker
get_tickers_info_step_checker >> get_tickers_financials_step_adder
get_tickers_financials_step_adder >> get_tickers_financials_step_checker
get_tickers_financials_step_checker >> get_tickers_balancesheet_step_adder
get_tickers_balancesheet_step_adder >> get_tickers_balancesheet_step_checker
get_tickers_balancesheet_step_checker >> get_tickers_cashflow_step_adder
get_tickers_cashflow_step_adder >> get_tickers_cashflow_step_checker
get_tickers_cashflow_step_checker >> get_tickers_eps_step_adder
get_tickers_eps_step_adder >> get_tickers_eps_step_checker
get_tickers_eps_step_checker >> get_tickers_earnings_step_adder
get_tickers_earnings_step_adder >> get_tickers_earnings_step_checker
get_tickers_earnings_step_checker >> get_tickers_actions_step_adder
get_tickers_actions_step_adder >> get_tickers_actions_step_checker
get_tickers_actions_step_checker >> get_tickers_major_holders_step_adder
get_tickers_major_holders_step_adder >> get_tickers_major_holders_step_checker
get_tickers_major_holders_step_checker >> get_tickers_holders_step_adder
get_tickers_holders_step_adder >> get_tickers_holders_step_checker
get_tickers_holders_step_checker >> get_tickers_analysis_step_adder
get_tickers_analysis_step_adder >> get_tickers_analysis_step_checker
get_tickers_analysis_step_checker >> get_tickers_isin_step_adder
get_tickers_isin_step_adder >> get_tickers_isin_step_checker
get_tickers_isin_step_checker >> get_tickers_news_step_adder
get_tickers_news_step_adder >> get_tickers_news_step_checker
get_tickers_news_step_checker >> get_tickers_recommendations_step_adder
get_tickers_recommendations_step_adder >> get_tickers_recommendations_step_checker
get_tickers_recommendations_step_checker >> get_tickers_shares_step_adder
get_tickers_shares_step_adder >> get_tickers_shares_step_checker
get_tickers_shares_step_checker >> get_tickers_options_step_adder
get_tickers_options_step_adder >> get_tickers_options_step_checker
get_tickers_options_step_checker >> get_tickers_historical_prices_step_adder
get_tickers_historical_prices_step_adder >> get_tickers_historical_prices_step_checker
get_tickers_historical_prices_step_checker >> get_tickers_tweets_step_adder
get_tickers_tweets_step_adder >> get_tickers_tweets_step_checker
get_tickers_tweets_step_checker >> api_data_quality_ctrl_step_adder
api_data_quality_ctrl_step_adder >> api_data_quality_ctrl_step_checker
api_data_quality_ctrl_step_checker >> cluster_end
cluster_end >> datalake_update_trigger
datalake_update_trigger >>  end_operator


