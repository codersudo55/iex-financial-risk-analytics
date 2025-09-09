from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('financial_pipeline', start_date=datetime(2025,1,1), schedule_interval='@daily', catchup=False) as dag:
    ingest = BashOperator(task_id='ingest', bash_command='python spark/jobs/ingest_raw.py')
    transform = BashOperator(task_id='transform', bash_command='python spark/jobs/transform_curated.py')
    score = BashOperator(task_id='score', bash_command='python spark/jobs/score_fraud.py')
    ingest >> transform >> score
