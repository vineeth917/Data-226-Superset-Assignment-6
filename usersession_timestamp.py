from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn, conn.cursor()

with DAG(
    dag_id='create_and_load_superset_tables',
    schedule='@daily',
    start_date=datetime(2024, 9, 25),
    catchup=False,
    default_args=default_args,
    description='Create and load user_session_channel and session_timestamp into Superset.raw'
) as dag:

    @task()
    def create_tables_and_stage():
        conn, cur = return_snowflake_conn()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Superset.raw.user_session_channel (
                    userId INT NOT NULL,
                    sessionId VARCHAR(32) PRIMARY KEY,
                    channel VARCHAR(32) DEFAULT 'direct'
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Superset.raw.session_timestamp (
                    sessionId VARCHAR(32) PRIMARY KEY,
                    ts TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE OR REPLACE STAGE Superset.raw.blob_stage
                URL = 's3://s3-geospatial/readonly/'
                FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
            """)
            conn.commit()
            logging.info("Tables and stage created successfully in a single transaction.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error creating tables or stage: {e}")
            raise
        finally:
            cur.close()

    @task()
    def load_user_session_channel():
        conn, cur = return_snowflake_conn()
        try:
            cur.execute("""
                COPY INTO Superset.raw.user_session_channel
                FROM @Superset.raw.blob_stage/user_session_channel.csv
                FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
            """)
            conn.commit()
            logging.info("Loaded data into user_session_channel.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error loading user_session_channel: {e}")
            raise
        finally:
            cur.close()

    @task()
    def load_session_timestamp():
        conn, cur = return_snowflake_conn()
        try:
            cur.execute("""
                COPY INTO Superset.raw.session_timestamp
                FROM @Superset.raw.blob_stage/session_timestamp.csv
                FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
            """)
            conn.commit()
            logging.info("Loaded data into session_timestamp.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error loading session_timestamp: {e}")
            raise
        finally:
            cur.close()

    # Task flow
    create_tables_and_stage() >> load_user_session_channel() >> load_session_timestamp()
