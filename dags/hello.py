from datetime import datetime 

from airflow.sdk import DAG, task

with DAG(dag_id='demo', start_date=datetime(2026, 8, 10), schedule="0 0 * * *") as dag:
    @task()
    def say_hi():
        print("yo whazzup")
    
    say_hi()