from airflow import DAG
from airflow.utils.dates import datetime
import pandas as pd


import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from custom_operators.operators.fetch_ratings import fetch_ratings
from custom_operators.operators.rank_movies import rank_movies

with DAG(
    dag_id="custom_operators",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    fetch_ratings >> rank_movies