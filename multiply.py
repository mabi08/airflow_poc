
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow(),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'provide_context': True
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("simple", default_args=default_args, schedule_interval=timedelta(1))


def add(**kwargs):
    n1 = kwargs['num1']
    n2 = kwargs['num2']
    return n1 + n2


def multi(**context):
    addition = context['task_instance'].xcom_pull(task_ids='add')
    return addition * context['num1']


t1 = PythonOperator(
    task_id='add', python_callable=add, op_kwargs={
        'num1': 5,
        'num2': 3
    }, dag=dag)

t2 = PythonOperator(
    task_id='multi', python_callable=multi, op_kwargs={
        'num1': 7,
    }, dag=dag)

t1.set_downstream(t2)
