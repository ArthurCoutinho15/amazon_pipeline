import sys
sys.path.append('pipeline')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import sys

SCRIPT_PATH = "/home/arthur/Projetos/amazon_pipeline/src/main.py"

def run_script():
    os.system(f"python {SCRIPT_PATH}")

default_args = {
    'owner': 'Arthur',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='amazon_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['etl']
) as dag:

    executar_script = PythonOperator(
        task_id='etl',
        python_callable=run_script,
    )
    
    executar_dbt = BashOperator(
        task_id="dbt_run",
        bash_command=" cd /home/arthur/Projetos/amazon_pipeline/dbt/amazon_data_warehouse && dbt run"
    )

    executar_script >> executar_dbt