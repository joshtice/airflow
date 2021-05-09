from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'catchup': False,
    'depends_on_past': False,
    'email_on_failure': False,
    'owner': 'udacity',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now()
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    aws_conn_id='aws_conn_id',
    redshift_conn_id='redshift_conn_id',
    iam_role="arn:aws:iam::850743350707:role/udacity-redshift-role",
    s3_bucket='udacity-dend',
    s3_key='log-data',
    table='staging_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    aws_conn_id='aws_conn_id',
    redshift_conn_id='redshift_conn_id',
    iam_role="arn:aws:iam::850743350707:role/udacity-redshift-role",
    s3_bucket='udacity-dend',
    s3_key='song-data',
    table='staging_songs',
    dag=dag
)

# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> end_operator
start_operator >> stage_songs_to_redshift >> end_operator
