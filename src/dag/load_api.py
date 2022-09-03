import logging

import pendulum
from airflow.decorators import dag, task
import psycopg2
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
import datetime
import requests
import json
import pandas as pd
log = logging.getLogger(__name__)

headers = {
    'X-Nickname': "durovver",
    'X-Cohort': "3",
    'X-API-KEY': "25c27781-8fde-4b30-a22e-524044a7580f"
}

with DAG(
        'load_api',
        schedule_interval='0/15 * * * *',
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False,
        tags=['sprint5', 'example'],
        is_paused_upon_creation=False
) as dag:

    def from_api_rest():
        url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants"
        response = requests.get(url, headers=headers).json()

        df = pd.DataFrame(response)
        df.rename(columns={'_id': 'id'}, inplace=True)
        df.set_index('id')
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.restaurants', df.values,
                                                                             target_fields=df.columns.tolist(),
                                                                             commit_every=100,
                                                                             replace=True, replace_index='id')

    def from_api_cour():
        url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers"
        response = requests.get(url, headers=headers).json()

        df = pd.DataFrame(response)
        df.rename(columns={'_id': 'id'}, inplace=True)
        df.set_index('id')
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.couriers', df.values,
                                                                             target_fields=df.columns.tolist(),
                                                                             commit_every=100,
                                                                             replace=True, replace_index='id')

    def from_api_deli():
        url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries"
        response = requests.get(url, headers=headers).json()

        df = pd.DataFrame(response)
        df.set_index('order_id')
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.deliveries', df.values,
                                                                             target_fields=df.columns.tolist(),
                                                                             commit_every=100,
                                                                             replace=True, replace_index='order_id')



    from_api_rest = PythonOperator(
        task_id='from_api_rest',
        python_callable=from_api_rest)

    from_api_cour = PythonOperator(
        task_id='from_api_cour',
        python_callable=from_api_cour)

    from_api_deli = PythonOperator(
        task_id='from_api_deli',
        python_callable=from_api_deli)

    from_api_rest >> from_api_cour >> from_api_deli