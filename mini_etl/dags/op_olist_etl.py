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
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine


from src import config
from src.transform import run_queries
from src.extract import extract
from src.load import load



with DAG(
    dag_id="olist_etl",
    default_args={"owner":"airflow"},
    start_date=airflow.utils.dates.days_ago(14),
    schedule=None,
    catchup=False,
    tags=["mini_etl"]
    ) as dag:

    greetings = BashOperator(
        task_id = "greetings",
        bash_command = """
    echo "Hello, World!"
        """,
        dag=dag
    )

    def extract_task(ti):
        csv_folder = config.DATASET_ROOT_PATH
        public_holidays_url = config.PUBLIC_HOLIDAYS_URL
        csv_table_mapping = config.get_csv_to_table_mapping()
        dataframes = extract(csv_folder, csv_table_mapping, public_holidays_url)
        ti.xcom_push(key="dataframes", value=dataframes)
        

    def transform_task(ti):
        dataframes = ti.xcom_pull(key="dataframes", task_ids=["extract_task"])[0]
        database = create_engine(rf"sqlite:///{config.SQLITE_BD_ABSOLUTE_PATH}", echo=False)
        load(dataframes, database)
        ti.xcom_push(key="database_con", value=str(database.url))


    def load_task(ti):
        database_con = ti.xcom_pull(key="database_con", task_ids=["transform_task"])[0]
        engine = create_engine(database_con)
        results = run_queries(engine)
        ti.xcom_push(key="results", value=results)


    extract_operator = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task,
        dag=dag
    )

    transform_operator = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task,
        dag=dag
    )

    load_operator = PythonOperator(
        task_id="load_task",
        python_callable=load_task,
        dag=dag
    )

    greetings >> extract_operator >> transform_operator >> load_operator
