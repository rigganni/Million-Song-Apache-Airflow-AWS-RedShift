from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                PostgresOperator)
from helpers import SqlQueries
import logging

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'email_on_retry': False,
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          schedule_interval='@daily',
          catchup=True
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

logging.info("Create Redshift tables")
# Adapted from https://knowledge.udacity.com/questions/163614
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift",
    autocommit="True"
)

logging.info("Load staging_events data from S3 to Redshift")
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    json_header="s3://udacity-dend/log_json_path.json"
)

logging.info("Load staging_songs data from S3 to Redshift")
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    #s3_key="song_data/A/A/A"
    s3_key="song_data"
)


logging.info("Load songplays table")
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

logging.info("Load users table")
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_query=SqlQueries.user_table_insert,
    truncate_table=False
)

logging.info("Load song table")
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_query=SqlQueries.song_table_insert,
    truncate_table=False
)

logging.info("Load artist table")
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_query=SqlQueries.artist_table_insert,
    truncate_table=False
)

logging.info("Load time table")
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert,
    truncate_table=False
)

# Below is a list of SQL data quality checks to be performed
dq_checks = [
        {"description": "Check for NULL in users",
         "check_sql": "SELECT COUNT(1) AS cnt FROM users WHERE userid IS NULL",
         "expected_result": 0},
        {"description": "Check for NULL in songs",
         "check_sql": "SELECT COUNT(1) AS cnt FROM songs WHERE songid IS NULL",
         "expected_result": 0},
        {"description": "Ensure record count in songplays is less than or equal to staging_events record count",
         "check_sql": "select (select count(1) from songplays) <= (select count(1) from staging_events);",
         "expected_result": True}
        ]

logging.info("Run data quality checks")
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    data_quality_checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift >> load_songplays_table
create_tables_task >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table  >> run_quality_checks
load_songplays_table >> load_song_dimension_table  >> run_quality_checks
load_songplays_table >> load_artist_dimension_table  >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
