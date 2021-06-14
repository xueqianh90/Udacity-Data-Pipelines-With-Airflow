from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'catchup': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# Load events data from S3 to RedShift.
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id = 'redshift',
    table = 'staging_events',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    file_format = 's3://udacity-dend/log_json_path.json',
    create_stmt = SqlQueries.create_table_staging_events
)

# Load songs data from S3 to RedShift.
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id = 'redshift',
    table = 'staging_songs',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/A/A',
    file_format = 'auto',
    create_stmt = SqlQueries.create_table_staging_songs
)

# Insert data to songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id = 'redshift',
    table = 'songplays',
    variables = '(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)',
    insert_query = SqlQueries.songplay_table_insert,
    create_stmt = SqlQueries.create_table_songplays
)

# Insert data to users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table = 'users',
    variables = '(userid, first_name, last_name, gender, level)',
    insert_query = SqlQueries.user_table_insert,
    create_stmt = SqlQueries.create_table_users,
    drop_stmt = SqlQueries.user_table_drop,
    truncate = False
)

# Insert data to songs table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table = 'songs',
    variables = '(songid, title, artistid, year, duration)',
    insert_query = SqlQueries.song_table_insert,
    create_stmt = SqlQueries.create_table_songs,
    drop_stmt = SqlQueries.song_table_drop,
    truncate = False
)

# Insert data to artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table = 'artists',
    variables = '(artistid, name, location, latitude, longitude)',
    insert_query = SqlQueries.artist_table_insert,
    create_stmt = SqlQueries.create_table_artist,
    drop_stmt = SqlQueries.artist_table_drop,
    truncate = False
)

# Insert data to time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table = 'time',
    variables = '(start_time, hour, day, week, month, year, weekday)',
    insert_query = SqlQueries.time_table_insert,
    create_stmt = SqlQueries.create_table_time,
    drop_stmt = SqlQueries.time_table_drop,
    truncate = False
)

# Check data quality
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    tables=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Steps

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
