from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 aws_conn_id="",
                 redshift_conn_id="",
                 iam_role="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 table="",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.iam_role = iam_role
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.table = table
        self.ignore_headers = ignore_headers
        

    def execute(self, context):
        self.log.info("Initializing redshift staging event")

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Loading data from {self.s3_bucket}/{self.s3_key}...")
        keys = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_key)
        for key in keys:
            if key.endswith('.json'):
                self.log.info(key)
                redshift_hook.run(
                    f"""
                        COPY {self.table}
                        FROM 's3://{self.s3_bucket}/{key}'
                        CREDENTIALS 'aws_iam_role={self.iam_role}'
                        JSON 'auto ignorecase'
                    """
                )

        
        





