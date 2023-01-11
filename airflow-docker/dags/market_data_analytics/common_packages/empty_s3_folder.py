from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow import AirflowException


class EmptyS3Folder(BaseOperator):
    """
    Operator to empty AWS bucker folder.
    :aws_conn_id           aws credentials stored in airflow connections
    :folder                folder name
    :bucket_name           The name of the S3 bucket
    """
    
    @apply_defaults
    def __init__(
        self,\
        aws_conn_id,\
        bucket_name,\
        folder,\
        *args, **kwargs
        ):

        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.folder = folder

    def execute(self, context):
        s3_hook = S3Hook(self.aws_conn_id)
        keys = s3_hook.list_keys(bucket_name = self.bucket_name,\
                                 prefix=self.folder ,\
                                 delimiter='', page_size=None, max_items=None)
        self.log.info(f"bucket_name :{self.bucket_name} /prefix :{self.folder }")
        if len(keys)!=0 :
            try:
                self.log.info(f'removal of keys from bucket/folder')
                s3_hook.delete_objects(bucket = self.bucket_name, keys = keys)
            except:
                self.log.info(f'Unable to remove keys from bucket/folder')
                raise ValueError('Unable to remove keys from bucket/folder')
        else:
            self.log.info(f'no bucket/folder')