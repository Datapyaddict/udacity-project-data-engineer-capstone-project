from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# from operators.empty_s3_folder import EmptyS3Folder
# from market_data_analytics.common_packages.empty_s3_folder import EmptyS3Folder
# from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
# from airflow.providers.amazon.aws.operators.s3 import S3DeleteBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator

#from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor

import configparser


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
            'Args': ['pip3', 'install', '--user', 'yfinance']
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
]


PUT_MARKET_DIM_DL_STEPS =  [

    {
        'Name': 'run put_market_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_market_dim.py']
        }
    }        
]

PUT_EXCHANGES_DIM_DL_STEPS =  [

    {
        'Name': 'run put_exchanges_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_exchanges_dim.py']
        }
    }        
]

PUT_TICKERS_INFO_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_info_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_info_dim.py']
        }
    }        
]

PUT_TICKERS_NEWS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_info_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_news_dim.py']
        }
    }        
]

PUT_TICKERS_FINANCIALS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_financials_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_financials_dim.py']
        }
    }        
]

PUT_TICKERS_BALANCESHEET_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_balancesheet_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_balancesheet_dim.py']
        }
    }        
]

PUT_TICKERS_CASHFLOW_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_cashflow_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_cashflow_dim.py']
        }
    }        
]

PUT_TICKERS_EPS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_eps_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_eps_dim.py']
        }
    }        
]

PUT_TICKERS_EARNINGS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_earnings_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_earnings_dim.py']
        }
    }        
]

PUT_TICKERS_ACTIONS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_actions_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_actions_dim.py']
        }
    }        
]

PUT_TICKERS_HOLDERS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_holders_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_holders_dim.py']
        }
    }        
]

PUT_TICKERS_ANALYSIS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_analysis_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_analysis_dim.py']
        }
    }        
]

PUT_TICKERS_RECOMMENDATIONS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_recommendations_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_recommendations_dim.py']
        }
    }        
]

PUT_TICKERS_SHARES_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_shares_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_shares_dim.py']
        }
    }        
]

PUT_TICKERS_OPTIONS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_options_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_options_dim.py']
        }
    }        
]

PUT_TICKERS_HISTORICAL_PRICES_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_historical_prices_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_historical_prices_dim.py']
        }
    }        
]

PUT_TICKERS_TWEETS_DIM_DL_STEPS =  [

    {
        'Name': 'run put_tickers_tweets_dim script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/put_tickers_tweets_dim.py']
        }
    }        
]

DATALAKE_QUALITY_CTRL_STEPS =  [

    {
        'Name': 'run datalake_quality_control script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit','/home/hadoop/datalake_quality_ctrl.py']
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
            # {
            #     'Name': "workers",
            #     'Market': 'ON_DEMAND',
            #     'InstanceRole': 'CORE',
            #     'InstanceType': EMR_INSTANCE_TYPE,
            #     'InstanceCount': EMR_NUM_WORKERS,
            # }

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

child_dag = DAG('atn_update_datalake',
          default_args=default_args,
          description='tbd',
#           schedule_interval='0 * * * *',       
          schedule_interval=None,
          tags=['pipeline'],
        )


start_operator = DummyOperator(task_id='Start_workflow',  dag=child_dag)

#upload updated script to s3

datalake_quality_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_datalake_quality_py_to_s3",
        filename="/opt/airflow/config/datalake_quality_ctrl.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'datalake_quality_ctrl.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

dl_config_cfg_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_dl_config_cfg_to_s3",
        filename="/opt/airflow/config/dl_config.cfg",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'dl_config.cfg'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

dl_config_cfg_removal_from_s3 = S3DeleteObjectsOperator(
        task_id="remove_dl_config_cfg_from_s3",
        bucket=DL_BUCKET_NAME,
        keys=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'dl_config.cfg'),
        aws_conn_id = "aws_credentials",
        verify=False,
        dag = child_dag
    )

common_modules_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_common_modules_py_to_s3",
        filename="/opt/airflow/config/common_modules.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'common_modules.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_market_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_market_dim_to_s3",
        filename="/opt/airflow/config/put_market_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_market_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_exchanges_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_exchanges_dim_to_s3",
        filename="/opt/airflow/config/put_exchanges_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_exchanges_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_info_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_info_to_s3",
        filename="/opt/airflow/config/put_tickers_info_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_info_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_news_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_news_to_s3",
        filename="/opt/airflow/config/put_tickers_news_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_news_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_financials_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_financials_to_s3",
        filename="/opt/airflow/config/put_tickers_financials_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_financials_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_balancesheet_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_balancesheet_to_s3",
        filename="/opt/airflow/config/put_tickers_balancesheet_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_balancesheet_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_cashflow_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_cashflow_to_s3",
        filename="/opt/airflow/config/put_tickers_cashflow_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_cashflow_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_eps_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_eps_to_s3",
        filename="/opt/airflow/config/put_tickers_eps_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_eps_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_earnings_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_earnings_to_s3",
        filename="/opt/airflow/config/put_tickers_earnings_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_earnings_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_actions_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_actions_to_s3",
        filename="/opt/airflow/config/put_tickers_actions_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_actions_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_holders_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_holders_to_s3",
        filename="/opt/airflow/config/put_tickers_holders_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_holders_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_analysis_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_analysis_to_s3",
        filename="/opt/airflow/config/put_tickers_analysis_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_analysis_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_recommendations_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_recommendations_to_s3",
        filename="/opt/airflow/config/put_tickers_recommendations_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_recommendations_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_shares_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_shares_to_s3",
        filename="/opt/airflow/config/put_tickers_shares_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_shares_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_options_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_options_to_s3",
        filename="/opt/airflow/config/put_tickers_options_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_options_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_historical_prices_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_historical_prices_to_s3",
        filename="/opt/airflow/config/put_tickers_historical_prices_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_historical_prices_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

put_tickers_tweets_dim_py_to_s3_uploader = LocalFilesystemToS3Operator(
        task_id="upload_put_tickers_tweets_to_s3",
        filename="/opt/airflow/config/put_tickers_tweets_dim.py",
        dest_key=os.path.join(DL_BUCKET_SCRIPTS_FOLDER,'put_tickers_tweets_dim.py'),
        dest_bucket=DL_BUCKET_NAME,
        replace=True,
        aws_conn_id = "aws_credentials",
        dag = child_dag
    )

# create EMR

emr_creator = EmrCreateJobFlowOperator(
        task_id='create_second_emr',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_credentials',
        emr_conn_id='aws_credentials',
        region_name = REGION,
        dag = child_dag
    )

# add steps to emr

init_step_adder = EmrAddStepsOperator(
    task_id='add_init_step_to_second_emr',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=INIT_STEPS,
    dag=child_dag
)



put_market_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_market_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_MARKET_DIM_DL_STEPS,
    dag=child_dag
)

put_market_dim_step_checker = EmrStepSensor(
        task_id='check_put_market_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_market_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_exchanges_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_exchanges_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_EXCHANGES_DIM_DL_STEPS,
    dag=child_dag
)

put_exchanges_dim_step_checker = EmrStepSensor(
        task_id='check_put_exchanges_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_exchanges_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_info_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_info_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_INFO_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_info_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_info_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_info_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_news_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_news_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_NEWS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_news_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_news_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_news_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_financials_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_financials_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_FINANCIALS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_financials_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_financials_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_financials_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_balancesheet_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_balancesheet_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_BALANCESHEET_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_balancesheet_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_balancesheet_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_balancesheet_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_cashflow_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_cashflow_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_CASHFLOW_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_cashflow_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_cashflow_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_cashflow_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_eps_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_eps_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_EPS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_eps_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_eps_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_eps_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_earnings_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_earnings_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_EARNINGS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_earnings_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_earnings_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_earnings_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_actions_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_actions_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_ACTIONS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_actions_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_actions_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_actions_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_holders_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_holders_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_HOLDERS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_holders_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_holders_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_holders_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_analysis_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_analysis_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_ANALYSIS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_analysis_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_analysis_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_analysis_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_recommendations_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_recommendations_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_RECOMMENDATIONS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_recommendations_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_recommendations_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_recommendations_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_shares_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_shares_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_SHARES_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_shares_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_shares_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_shares_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_options_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_options_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_OPTIONS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_options_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_options_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_options_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_historical_prices_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_historical_prices_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_HISTORICAL_PRICES_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_historical_prices_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_historical_prices_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_historical_prices_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

put_tickers_tweets_dim_step_adder = EmrAddStepsOperator(
    task_id='add_put_tickers_tweets_dim_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=PUT_TICKERS_TWEETS_DIM_DL_STEPS,
    dag=child_dag
)

put_tickers_tweets_dim_step_checker = EmrStepSensor(
        task_id='check_put_tickers_tweets_dim_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_put_tickers_tweets_dim_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )

datalake_quality_ctrl_step_adder = EmrAddStepsOperator(
    task_id='add_datalake_quality_ctrl_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=DATALAKE_QUALITY_CTRL_STEPS,
    dag=child_dag
)

datalake_quality_ctrl_step_checker = EmrStepSensor(
        task_id='check_datalake_quality_ctrl_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
        target_states=['COMPLETED'],
        step_id = "{{ task_instance.xcom_pull(task_ids='add_datalake_quality_ctrl_step', key='return_value')[0]}}",
        aws_conn_id='aws_credentials',
        dag=child_dag
    )



cluster_end = EmrTerminateJobFlowOperator(
    task_id='end_emr_cluster',
    trigger_rule='all_done',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_second_emr', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=child_dag
)

end_operator = DummyOperator(task_id='Stop_workflow',  dag=child_dag)


# tasks dependencies

start_operator >> dl_config_cfg_to_s3_uploader
dl_config_cfg_to_s3_uploader >> datalake_quality_py_to_s3_uploader
datalake_quality_py_to_s3_uploader >> common_modules_py_to_s3_uploader
common_modules_py_to_s3_uploader >> put_market_dim_py_to_s3_uploader
put_market_dim_py_to_s3_uploader >> put_exchanges_dim_py_to_s3_uploader
put_exchanges_dim_py_to_s3_uploader >> put_tickers_info_dim_py_to_s3_uploader
put_tickers_info_dim_py_to_s3_uploader >> put_tickers_news_dim_py_to_s3_uploader
put_tickers_news_dim_py_to_s3_uploader >> put_tickers_financials_dim_py_to_s3_uploader
put_tickers_financials_dim_py_to_s3_uploader >> put_tickers_balancesheet_dim_py_to_s3_uploader
put_tickers_balancesheet_dim_py_to_s3_uploader >> put_tickers_cashflow_dim_py_to_s3_uploader
put_tickers_cashflow_dim_py_to_s3_uploader >> put_tickers_eps_dim_py_to_s3_uploader
put_tickers_eps_dim_py_to_s3_uploader >> put_tickers_earnings_dim_py_to_s3_uploader
put_tickers_earnings_dim_py_to_s3_uploader >> put_tickers_actions_dim_py_to_s3_uploader
put_tickers_actions_dim_py_to_s3_uploader >> put_tickers_holders_dim_py_to_s3_uploader
put_tickers_holders_dim_py_to_s3_uploader >> put_tickers_analysis_dim_py_to_s3_uploader
put_tickers_analysis_dim_py_to_s3_uploader >> put_tickers_recommendations_dim_py_to_s3_uploader
put_tickers_recommendations_dim_py_to_s3_uploader >> put_tickers_shares_dim_py_to_s3_uploader
put_tickers_shares_dim_py_to_s3_uploader >> put_tickers_options_dim_py_to_s3_uploader
put_tickers_options_dim_py_to_s3_uploader >> put_tickers_historical_prices_dim_py_to_s3_uploader
put_tickers_historical_prices_dim_py_to_s3_uploader >> put_tickers_tweets_dim_py_to_s3_uploader
put_tickers_tweets_dim_py_to_s3_uploader >> emr_creator
emr_creator >> init_step_adder
init_step_adder >>  put_market_dim_step_adder
put_market_dim_step_adder >> put_market_dim_step_checker
put_market_dim_step_checker >> put_exchanges_dim_step_adder
put_exchanges_dim_step_adder >> put_exchanges_dim_step_checker
put_exchanges_dim_step_checker >> put_tickers_info_dim_step_adder
put_tickers_info_dim_step_adder >> put_tickers_info_dim_step_checker
put_tickers_info_dim_step_checker >> put_tickers_news_dim_step_adder
put_tickers_news_dim_step_adder >> put_tickers_news_dim_step_checker
put_tickers_news_dim_step_checker >> put_tickers_financials_dim_step_adder
put_tickers_financials_dim_step_adder >> put_tickers_financials_dim_step_checker
put_tickers_financials_dim_step_checker >> put_tickers_balancesheet_dim_step_adder
put_tickers_balancesheet_dim_step_adder >> put_tickers_balancesheet_dim_step_checker
put_tickers_balancesheet_dim_step_checker >> put_tickers_cashflow_dim_step_adder
put_tickers_cashflow_dim_step_adder >> put_tickers_cashflow_dim_step_checker
put_tickers_cashflow_dim_step_checker >> put_tickers_eps_dim_step_adder
put_tickers_eps_dim_step_adder >> put_tickers_eps_dim_step_checker
put_tickers_eps_dim_step_checker >> put_tickers_earnings_dim_step_adder
put_tickers_earnings_dim_step_adder >> put_tickers_earnings_dim_step_checker
put_tickers_earnings_dim_step_checker >> put_tickers_actions_dim_step_adder
put_tickers_actions_dim_step_adder >> put_tickers_actions_dim_step_checker
put_tickers_actions_dim_step_checker >> put_tickers_holders_dim_step_adder
put_tickers_holders_dim_step_adder >> put_tickers_holders_dim_step_checker
put_tickers_holders_dim_step_checker >> put_tickers_analysis_dim_step_adder
put_tickers_analysis_dim_step_adder >> put_tickers_analysis_dim_step_checker
put_tickers_analysis_dim_step_checker >> put_tickers_recommendations_dim_step_adder
put_tickers_recommendations_dim_step_adder >> put_tickers_recommendations_dim_step_checker
put_tickers_recommendations_dim_step_checker >> put_tickers_shares_dim_step_adder
put_tickers_shares_dim_step_adder >> put_tickers_shares_dim_step_checker
put_tickers_shares_dim_step_checker >> put_tickers_options_dim_step_adder
put_tickers_options_dim_step_adder >> put_tickers_options_dim_step_checker
put_tickers_options_dim_step_checker >> put_tickers_historical_prices_dim_step_adder
put_tickers_historical_prices_dim_step_adder >> put_tickers_historical_prices_dim_step_checker
put_tickers_historical_prices_dim_step_checker >> put_tickers_tweets_dim_step_adder
put_tickers_tweets_dim_step_adder >> put_tickers_tweets_dim_step_checker
put_tickers_tweets_dim_step_checker >> datalake_quality_ctrl_step_adder
datalake_quality_ctrl_step_adder >> datalake_quality_ctrl_step_checker
datalake_quality_ctrl_step_checker >> cluster_end
cluster_end >> dl_config_cfg_removal_from_s3
dl_config_cfg_removal_from_s3  >> end_operator
