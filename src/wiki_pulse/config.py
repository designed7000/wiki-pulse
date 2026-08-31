"""
Constants for the Airflow DAGs

Retention must exceed the anomaly lookback, or detection queries a window that 
has been partially deleted and silently computes the wrong threshold
"""

ANOMALY_LOOKBACK_HOURS = 24
ANOMALY_PERCENTILE = 99.5

RETENTION_DAYS = 15
DELETE_BATCH_SIZE = 10_000

assert RETENTION_DAYS * 24 > ANOMALY_LOOKBACK_HOURS, (
    "retention would delete data the anomaly DAG still reads"
)