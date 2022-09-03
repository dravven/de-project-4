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
        'load_dds',
        schedule_interval='0/15 * * * *',
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False,
        tags=['sprint5', 'example'],
        is_paused_upon_creation=False
) as dag:


    def restaurants():
        conn = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

        conn.run(sql=f"""
            insert into dds.restaurants(restaurants_id, name)
            select * from stg.restaurants
            on conflict (restaurants_id) DO UPDATE
            set name = excluded.name
            """)


    def couriers():
        conn = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

        conn.run(sql=f"""
            insert into dds.couriers(courier_id, name, rate)
            select id, name, avg(coalesce(rate, 0)) as rate from stg.couriers cr
                left join stg.deliveries dv on dv.courier_id = cr.id
            group by id, name
            on conflict (courier_id) DO UPDATE
            set name = excluded.name,
            rate = excluded.rate
            """)


    def orders():
        conn = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

        conn.run(sql=f"""
            insert into dds.orders(order_id, courier_id, ts_id, delivery_id, restaurants_id, address, sum, tip_sum)
            select order_id, cr.id, tm.id, delivery_id, 1, address, sum, tip_sum  from stg.deliveries dv
            join dds.couriers cr on cr.courier_id = dv.courier_id
            join dds.time tm on tm.order_ts = dv.order_ts
            on conflict (order_id) do nothing;
            """)


    def time():
        conn = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

        conn.run(sql=f"""
            insert into dds.time(order_ts, delivery_ts, year, month)
            select order_ts, delivery_ts, extract(year from delivery_ts), extract(month from delivery_ts) 
                from stg.deliveries
            on conflict (order_ts) DO nothing
            """)


    def dm_courier_ledger():
        conn = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

        conn.run(sql=f"""
            insert into cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, 
                rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)

            select *, courier_order_sum + courier_tips_sum * 0.95 from(select ord.courier_id, cr.name, year, month, count(order_id), sum(sum), rate, sum(sum) * 0.25 as order_processing_fee,
       sum(case when rate < 4 then (case when (sum * 0.05 >= 100) then sum * 0.05 else 100 end)
            when rate between 4 and 4.49 then (case when (sum * 0.07 >= 150) then sum * 0.07 else 150 end)
            when rate between 4.5 and 4.89 then (case when (sum * 0.08 >= 175) then sum * 0.08 else 175 end)
            when rate >= 4.9 then (case when (sum * 0.1 >= 200) then sum * 0.1 else 200 end)
            end) as courier_order_sum, sum(tip_sum) as courier_tips_sum
                from dds.couriers cr
                left join dds.orders ord on ord.courier_id = cr.id
                join dds.time tm on tm.id = ord.ts_id
                group by ord.courier_id, cr.name, year, month, rate)t
            on conflict (courier_id) DO nothing
            """)



    restaurants = PythonOperator(
        task_id='restaurants',
        python_callable=restaurants)

    couriers = PythonOperator(
        task_id='couriers',
        python_callable=couriers)

    time = PythonOperator(
        task_id='time',
        python_callable=time)

    orders = PythonOperator(
        task_id='orders',
        python_callable=orders)

    dm_courier_ledger = PythonOperator(
        task_id='dm_courier_ledger',
        python_callable=dm_courier_ledger)

    restaurants >> couriers >> time >> orders >> dm_courier_ledger