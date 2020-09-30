import boto3
import numpy as np 
import pandas as pd 

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "y",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    #"retry_delay": timedelta(minutes=5),
    'provide_context': True
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("titanic", default_args=default_args, schedule_interval=timedelta(100))


def read_s3_data(**kwargs):
    print("start reading data")
    session = boto3.Session(
       
    )
 
    bucket = "datalake-eu-central-1-dev"    
    test_location = "ofx0-kubeflow/titanic/test.csv"
    train_location = "ofx0-kubeflow/titanic/train.csv"

    s3 = session.client('s3')
    test_response = s3.get_object(Bucket=bucket, Key=test_location)
    train_response = s3.get_object(Bucket=bucket, Key=train_location)

    test_df = pd.read_csv(test_response['Body'], delimiter=',')
    train_df = pd.read_csv(train_response['Body'], delimiter=',')

    print(train_df)
    return test_df, train_df


# def data_processing(**context):
#     test_df, train_df = context['task_instance'].xcom_pull(task_ids='read_s3_data')
#     data = [train_df, test_df]
#     for dataset in data:
#         dataset['relatives'] = dataset['SibSp'] + dataset['Parch']
#         dataset.loc[dataset['relatives'] > 0, 'not_alone'] = 0
#         dataset.loc[dataset['relatives'] == 0, 'not_alone'] = 1
#         dataset['not_alone'] = dataset['not_alone'].astype(int)
#     train_df['not_alone'].value_counts()

#     return addition * context['num1']


def data_processing(**context):
    print("t2")

t1 = PythonOperator(
    task_id='read_s3_data', python_callable=read_s3_data, op_kwargs={
    }, dag=dag)


t2 = PythonOperator(
    task_id='data_processing', python_callable=data_processing, op_kwargs={
    }, dag=dag)


t1.set_downstream(t2)
