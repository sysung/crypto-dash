from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_dbt_pipeline',
    default_args=default_args,
    description='Run crypto dbt models',
    schedule_interval=timedelta(minutes=10),
    catchup=False,
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    dag=dag,
)

dbt_run >> dbt_test
