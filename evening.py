from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator

import requests
import pandas as pd
from sqlalchemy import create_engine , text
import os
from dotenv import load_dotenv

load_dotenv()


def extract_articles(**kwargs):
    url = ('https://newsapi.org/v2/everything?'
            'q=Apple&'
            'from=2026-06-10&to=2026-06-11&'
            'sortBy=popularity&'
            f'apiKey={os.getenv('NEWS_API_KEY')}') 
 
    response=requests.get(url)
 
    data=response.json()
 
    # kwargs['ti'].xcom_push(key="extract", value=data) # pushing data explicitly to xcom
    return data
 
def transform_articles(**kwargs):
    articles = kwargs['ti'].xcom_pull(task_ids="extract")
 
    articles_list = articles['articles']
 
    articles_df = pd.DataFrame(articles_list)
 
    articles_df.drop(
        columns=['source', 'urlToImage'],
        inplace=True,
        errors='ignore'
    )
 
    articles_df.rename(
        columns={'publishedAt': 'published_at'},
        inplace=True
    )
 
    # Convert NaN values to None so XCom can serialize it as JSON null
    articles_df = articles_df.astype(object).where(pd.notna(articles_df), None)
 
    articles_records = articles_df.to_dict(orient='records')
 
    kwargs['ti'].xcom_push(key="transform", value=articles_records)
 
def load_articles(**kwargs):
    articles_records = kwargs['ti'].xcom_pull(task_ids = "transform",key="transform")
 
    articles_df = pd.DataFrame(articles_records)
 
    DATABASE_NAME = os.getenv('DATABASE_NAME')
    DATABASE_USER = os.getenv('DATABASE_USER')
    DATABASE_PORT = os.getenv('DATABASE_PORT')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE_HOST = os.getenv('DATABASE_HOST')
 
    engine = create_engine(f'postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}')
 
    with engine.connect() as conn:
        resort = conn.execute(text('select 1;'))
        for i in resort:
            print(i)
 
    articles_df.to_sql('articles',engine , if_exists='replace',index=False )
 
with DAG(
    'news_etl_dag',
    start_date=datetime(2026, 6, 10),
    schedule=timedelta(minutes=1),
    catchup=False
) as dag:
 
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_articles
    )
 
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_articles,
    )
 
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_articles,
    )
 
    extract_task >> transform_task >> load_task