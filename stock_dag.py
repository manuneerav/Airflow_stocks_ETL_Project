from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.dates import days_ago
from datetime import datetime
from stock_etl import run_stock_etl

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023, 6, 26),
    'retries':1,
    'email':'neeravnilay@gmail.com',
    'email_on_failure':False,
    'email_on_retry':False,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'stock_dag',
    default_args = default_args,
    description="My first etl dag."

)

run_etl = PythonOperator(
    task_id='complete_stock_etl',
    python_callable=run_stock_etl,
    dag=dag
)