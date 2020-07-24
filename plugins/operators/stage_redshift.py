from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql_command = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON {};
        """
    @apply_defaults
    
    def __init__(self,
                 redshift_conn_id="",
                 aws_conn_id="",
                 table = "",
                 s3_bucket = "",
                 region="",
                 data_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id=aws_conn_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.region=region
        self.data_format=data_format
        self.execution_date=kwargs.get('execution_date')
    
    def execute(self, context):
        aws=AwsHook(self.aws_conn_id)
        credentials = aws.get_credentials()
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from S3 bucket into Redshift database")
        
        
        '''Use Redshift Hook to copy data from S3 bucket'''
        redshift_hook.run(StageToRedshiftOperator.sql_command.format(self.table,
                                                                     self.s3_bucket,
                                                                     credentials.access_key,
                                                                     credentials.secret_key,
                                                                     self.region,
                                                                     self.data_format))