"""
Flag pages with unusually high edit velocity.

Threshold is a global percentile over all (page, minute) buckets in the lookback
window. A per-page baseline would be better but most pages are edited once, so
there's no history to work from.

Runs hourly over a longer lookback, so the threshold moves slowly between runs.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from wiki_pulse.utils import threshold_at_percentile

SELECT_COUNTS = """
    -- Reads the edit_count of every bucket in the lookback window 
    -- -> will be feeded in the threshold_at_percentile script to check for the anomalies
    
    SELECT edit_count FROM topic_velocity WHERE bucket BETWEEN %s AND %s
"""

INSERT_ANOMALIES = """
    -- That will run after we got the threshold.
    INSERT INTO anomalies (title, bucket, edit_count, bytes_delta_sum, 
                            distinct_users, threshold, percentile, lookback_hours)
    SELECT title, bucket, edit_count, bytes_delta_sum, 
                            distinct_users, %s, %s, %s
    FROM topic_velocity
    WHERE bucket BETWEEN %s AND %s AND edit_count > %s
    ON CONFLICT (title, bucket) DO NOTHING
    """

with DAG(
    dag_id="anomaly_detection",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    params={"percentile": 99.5, "lookback_hours": 24},
) as dag:
    
    @task()
    def detect(**context):
        percentile = context["params"]["percentile"]
        lookback_hours = context["params"]["lookback_hours"]
        
        window_end = context["data_interval_end"]
        window_start = window_end - timedelta(hours=lookback_hours)
        
        hook = PostgresHook(postgres_conn_id="wikipulse")
        
        rows = hook.get_records(SELECT_COUNTS, parameters=(window_start, window_end))
        counts = [r[0] for r in rows]
        
        threshold = threshold_at_percentile(counts, percentile)
        if threshold is None:
            print("no data in window, nothing to do")
            return

        hook.run(
            INSERT_ANOMALIES,
            parameters=(threshold, percentile, lookback_hours, window_start, window_end, threshold),
        )
        print(f"threshold={threshold} over {len(counts)} buckets")
        
    detect()
