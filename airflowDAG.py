from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import calendar
import time



default_args = {
    'start_date' : datetime(2022,2,2,18,0),
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
}

    
with DAG('ccxt_tokens_25', 
            schedule_interval='*/5 * * * *', 
            default_args=default_args,
            catchup=False,
            dagrun_timeout=timedelta(minutes=10)) as dag:

    incrementalLoadFromJson = BashOperator(
        task_id = 'incrementalLoadFromJson',
        bash_command='path to incremental load python script'
    )
    
    incrementalLoadFromJson
    
