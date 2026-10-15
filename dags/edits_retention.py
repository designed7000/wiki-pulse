""" 
This DAG is for deleting edits older than the retention cutoff.

edits table grows at roughly 35 mb/day with indexes and without any bounds
topic_velocity view only reads the last 24 hours, so the retention bounds what is kept

It runs daily and deletes in batches so a large backlog would not hold one long transaction
"""

from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task

from wiki_pulse.config import DELETE_BATCH_SIZE, RETENTION_DAYS

DELETE_BATCH = """
    DELETE FROM edits
    WHERE ctid IN (
        SELECT ctid FROM edits WHERE event_ts < %s LIMIT %s
    )
"""

with DAG(
    dag_id="edits_retention",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    params={"retention_days": RETENTION_DAYS},
) as dag:
    
    @task()
    def prune(**context):
        retention_days = context["params"]["retention_days"]
        cutoff = context["data_interval_end"] - timedelta(days=retention_days)
        
        hook = PostgresHook(postgres_conn_id="wikipulse")
        conn = hook.get_conn()
        deleted = 0
        
        with conn.cursor() as cur:
            while True:
                cur.execute(DELETE_BATCH, (cutoff, DELETE_BATCH_SIZE))
                batch = cur.rowcount
                conn.commit()
                deleted += batch
                if batch == 0:
                    break 
        
        print(f"deleted {deleted} rows older than {cutoff.isoformat()}")
    
    prune()
