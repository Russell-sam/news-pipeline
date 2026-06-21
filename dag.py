from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    "news_etl_pipeline",
    description="Triggers the News ETL pipeline",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["news", "etl"]
) as dag:
    
    run_main = BashOperator(
        task_id="run_news_etl_main",
        bash_command="/home/russell/etlenv/bin/python /home/russell/evening_class/pipeline.py"
    )
    
    run_main
