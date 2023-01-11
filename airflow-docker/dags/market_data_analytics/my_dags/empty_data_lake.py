from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# from operators.empty_s3_folder import EmptyS3Folder
from market_data_analytics.common_packages.empty_s3_folder import EmptyS3Folder

from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator

# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator

import configparser

config = configparser.ConfigParser()
config_file_path = '/opt/airflow/config/dl_config.cfg'
config.read_file(open(config_file_path))

os.environ['AWS_DEFAULT_REGION'] = config.get('CLUSTER', 'AWS_REGION')

DL_BUCKET_NAME = config.get('S3_BUCKET', 'DL_BUCKET_NAME')
DL_BUCKET_OUTPUT_FOLDER = config.get('S3_BUCKET', 'DL_BUCKET_OUTPUT_FOLDER')


dt = datetime.now()

default_args = {
    'owner': 'atn',
    'start_date': datetime(dt.year, dt.month, dt.day),
    #     "depends_on_past": False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    #     'catchup' : False,
    #     'email_on_retry' : False,
}

empty_datalake_dag = DAG('empty_datalake',
                 default_args=default_args,
                 description='tbd',
                 #           schedule_interval='0 * * * *',
                 schedule_interval='@once',
                 tags=['pipeline'],
                 )

start_operator = DummyOperator(task_id='Start_workflow', dag=empty_datalake_dag)

# empty bucket folders

empty_datalake_task = EmptyS3Folder(
    task_id='empty_datalake_folder',
    bucket_name=DL_BUCKET_NAME,
    folder=DL_BUCKET_OUTPUT_FOLDER,
    aws_conn_id="aws_credentials",
    dag=empty_datalake_dag,
)

end_operator = DummyOperator(task_id='Stop_workflow', dag=empty_datalake_dag)


# tasks dependencies

start_operator >> empty_datalake_task
empty_datalake_task >> end_operator


