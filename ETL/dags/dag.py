from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG('spark_submit_job', 
         start_date=datetime(2024, 4, 1),
         schedule_interval='@monthly') as dag:
    
    pass