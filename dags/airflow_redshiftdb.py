from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

'''Default arguments for the DAG are as follows:
    DAG does not have dependencies on past runs,
    The task is retried 3 times if it fails,
    The retries occur every 5 minutes,
    Catch up is turned off,
    No e-mail on retry
'''

default_args= {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':False,
    'retries':3,
    'retry_delay': 300,
    'catchup_by_default':False,
    'email_on_retry':False
}

dag = DAG('udacity_apache_airflow',
          default_args=default_args,
          description='Load and transform data into Redshift using Apache Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

'''Indicates beginning of dag'''
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

'''Initiate the creation of tables'''
create_staging_events_table = PostgresOperator(
    task_id='Create_staging_events',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_events_table_create
)

create_staging_songs_table = PostgresOperator(
    task_id='Create_staging_songs',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_songs_table_create
)

create_songplays_table = PostgresOperator(
    task_id='Create_songplays',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.songplay_table_create
)

create_artists_table = PostgresOperator(
    task_id='Create_artists',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.artist_table_create
)

create_songs_table = PostgresOperator(
    task_id='Create_songs',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.song_table_create
)

create_users_table = PostgresOperator(
    task_id='Create_users',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.user_table_create
)

create_time_table = PostgresOperator(
    task_id='Create_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.time_table_create
)

transient_operator = DummyOperator(task_id='Tables_created', dag=dag)


'''Staging events and songs data from Redshift'''
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Staging_events',
    dag=dag,
    table="staging_events",
    s3_bucket="s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    data_format="'s3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Staging_songs',
    dag=dag,
    table="staging_songs",
    s3_bucket="s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    data_format="'auto'"
)

'''Staging from S3 Bucket to Redshift'''
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_columns=SqlQueries.songplay_table_insert
)

'''Insert data from Redshift into fact and dimension tab'''
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_columns=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_columns=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_columns=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_columns=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_collection=["songplays","users","songs","artists","time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_staging_songs_table >> transient_operator
start_operator >> create_staging_events_table >> transient_operator
start_operator >> create_songplays_table >> transient_operator
start_operator >> create_artists_table >> transient_operator
start_operator >> create_songs_table >> transient_operator
start_operator >> create_users_table >> transient_operator
start_operator >> create_time_table >> transient_operator


transient_operator >> stage_events_to_redshift >> load_songplays_table
transient_operator >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table>> load_time_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
run_quality_checks >> end_operator