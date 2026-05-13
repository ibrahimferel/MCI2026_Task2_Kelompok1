from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mci_data_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='task2_orders_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1,
    description='Orders API -> Spark Analytics -> ClickHouse Warehouse'
) as dag:

    fetch_orders_data = BashOperator(
        task_id='fetch_orders_data',
        bash_command='python /opt/airflow/dags/scripts/fetch_task2_stream.py'
    )

    process_orders_analytics = BashOperator(
        task_id='process_orders_analytics',
        bash_command='python /opt/airflow/dags/scripts/process_task2_spark.py'
    )

    fetch_orders_data >> process_orders_analytics