import os, sys
from pathlib import Path

project_root="AI_Sprint_1"
project_name = "Sprint_1"
project_dir = os.path.join(project_root, project_name)

if str(Path(__file__).parent.parent).split("/")[-1] == project_name:
    sys.path.insert(0, str(Path(__file__).parent.parent))
elif str(Path(__file__).parent.parent.parent).split("/")[-1] == 'opt':
    sys.path.insert(0, str(Path(__file__).parent.parent))
else:
    sys.path.insert(0, os.path.join(os.path.expanduser("~"), project_dir))

import airflow
from airflow.decorators import dag, task
from sqlalchemy import create_engine

from src import config
from src.transform import run_queries
from src.extract import extract
from src.load import load

from itertools import count

nlog = count(1)


@dag(
    dag_id="olist_etl_decorator",
    default_args={"owner":"airflow"},
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None
    )
def olist_etl():
    
    @task
    def extract_task():
        print(f"({next(nlog)}) Ejecutando task: {__name__}")
        csv_folder = config.DATASET_ROOT_PATH
        print("csv_folder assigned")
        public_holidays_url = config.PUBLIC_HOLIDAYS_URL
        print("public_holidays_url assigned")
        csv_table_mapping = config.get_csv_to_table_mapping()
        print("csv_table_mapping assigned")
        dataframes = extract(csv_folder, csv_table_mapping, public_holidays_url)
        print("dataframes assigned")
        return dataframes
    
    @task
    def transform_task(dataframes):
        print(f"({next(nlog)}) Ejecutando task: {__name__}")
        database = create_engine(rf"sqlite:///{config.SQLITE_BD_ABSOLUTE_PATH}", echo=False)
        load(dataframes, database)
        # Obtener la cadena de conexión
        return str(database.url)
    
    @task
    def load_task(database_con):
        print(f"({next(nlog)}) Ejecutando task: {__name__}")
        engine = create_engine(database_con)
        return run_queries(engine)


    print(f"({next(nlog)}) Iniciando DAG: {__name__}")
    dataframes = extract_task()
    database_con = transform_task(dataframes)
    load_task(database_con)
    print(f"({next(nlog)}) Ejecución finalizada exitosamente.")

olist_etl()