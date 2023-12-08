from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'Deeplogic',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'FeedsKafka',
    default_args=default_args,
    description='Send feeds to Kafka',
    schedule_interval='0 * * * *',
    catchup=False,
)

# Task to run the Python script using BashOperator
run_script_task = BashOperator(
    task_id='feeds_kafka',
    bash_command='python3 /opt/airflow/feeds/feeds_kafka.py',
    dag=dag,
)
