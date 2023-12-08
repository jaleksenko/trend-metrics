from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import logging
logging.getLogger().setLevel(logging.INFO)

# Define default arguments for the DAG
default_args = {
    'owner': 'Deeplogic',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'AddChannels',
    default_args=default_args,
    description='Add feed channels from CSV to the database',
    schedule_interval='30 23 * * *',
    catchup=False,
)

# Define the task using BashOperator
run_script_task = BashOperator(
    task_id='run_add_channels',
    bash_command='python3 /opt/airflow/feeds/add_channels.py',
    dag=dag,
)

