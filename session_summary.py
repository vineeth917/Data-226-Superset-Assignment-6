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
    dag_id='elt_session_summary',
    schedule='@daily',
    start_date=datetime(2024, 9, 25),
    catchup=False,
    default_args=default_args,
    description='Create joined table session_summary under Superset.analytics'
) as dag:

    @task()
    def create_summary():
        conn, cur = return_snowflake_conn()
        try:
            # Create target table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Superset.analytics.session_summary (
                    sessionId VARCHAR(32) PRIMARY KEY,
                    userId INT,
                    channel VARCHAR(32),
                    ts TIMESTAMP
                );
            """)

            # Insert only non-duplicate records (avoid inserting same sessionId again)
            cur.execute("""
                INSERT INTO Superset.analytics.session_summary (sessionId, userId, channel, ts)
                SELECT
                    uc.sessionId,
                    uc.userId,
                    uc.channel,
                    st.ts
                FROM Superset.raw.user_session_channel uc
                JOIN Superset.raw.session_timestamp st
                    ON uc.sessionId = st.sessionId
                WHERE uc.sessionId NOT IN (
                    SELECT sessionId FROM Superset.analytics.session_summary
                );
            """)

            conn.commit()
            logging.info("Session summary created and populated.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error in session_summary creation: {e}")
            raise
        finally:
            cur.close()

    create_summary()
