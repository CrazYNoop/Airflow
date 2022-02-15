from airflow.models.baseoperator import BaseOperator
import datetime
import boto3


class S3_Operator(BaseOperator):
    def __init__(self, aws_access_key_id , aws_secret_access_key , file,s3_path, **kwargs) -> None:
        super().__init__(**kwargs)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.file = file
        self.s3_path=s3_path

    def execute(self,**kwargs):
        s3=boto3.client('s3',
                   aws_access_key_id = self.aws_access_key_id,
                   aws_secret_access_key = self.aws_secret_access_key)
        upload_aws_bucket = 'slmi'
        upload_file_key = 'airflow/' + str(self.s3_path)+'_'+str(datetime.datetime.now())
        s3.upload_file(self.file,upload_aws_bucket,upload_file_key)
        return upload_file_key
