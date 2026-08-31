""" 
Read-only HTTP API over the pipeline's output

The dashboard cannot talk to Postgres directly, and nothing downstream should 
depend on the table layout. This api acts as a contract in between.
"""

import os 
from datetime import datetime

import psycopg
from fastapi import FastAPI, Query, Depends
from pydantic import BaseModel 

DSN = os.getenv("POSTGRES_DSN", "postgresql://wiki:wiki@localhost:5432/wikipulse")

SELECT_VELOCITY = """ 
    SELECT title, bucket, edit_count, bytes_delta_sum, distinct_users
    FROM topic_velocity
    ORDER BY edit_count DESC, bucket DESC
    LIMIT %s
"""

SELECT_ANOMALIES = """ 
    SELECT title, bucket, edit_count, bytes_delta_sum, distinct_users,
           threshold, percentile, lookback_hours, flagged_at
    FROM anomalies
    ORDER BY bucket DESC
    LIMIT %s
"""


class VelocityRow(BaseModel):
    title: str
    bucket: datetime
    edit_count: int 
    bytes_delta_sum: int 
    distinct_users: int 

class AnomalyRow(VelocityRow):
    threshold: float 
    percentile: float
    lookback_hours: int 
    flagged_at: datetime 

def get_cursor():
    """One connection per request. Simple, yet survives the database restarting"""
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        yield cur 

app = FastAPI(title="wiki-pulse", description="Live Wikipedia edit velocity")

@app.get("/velocity", response_model=list[VelocityRow])
def get_velocity(limit: int = Query(20, ge=1, le=200), cur=Depends(get_cursor)):
    """Pages with the highest edit velocity in the last 24 hours."""
    cur.execute(SELECT_VELOCITY, (limit,))
    return [VelocityRow(**dict(zip(VelocityRow.model_fields, r))) for r in cur.fetchall()]


@app.get("/anomalies", response_model=list[AnomalyRow])
def get_anomalies(limit: int = Query(20, ge=1, le=200), cur=Depends(get_cursor)):
    """Most recently flagged velocity spikes."""
    cur.execute(SELECT_ANOMALIES, (limit,))
    return [AnomalyRow(**dict(zip(AnomalyRow.model_fields, r))) for r in cur.fetchall()]