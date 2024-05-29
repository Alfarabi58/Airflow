'''
=================================================
Milestone 3

Nama  : Alfarabi
Batch : BSD-005

This Airflow DAG (Directed Acyclic Graph) automates the process of fetching, cleaning, and analyzing customer shopping trends data.
The data is sourced from a PostgreSQL database, cleaned, and then stored in Elasticsearch for further analysis. 
The DAG ensures that this workflow is executed periodically as defined by the schedule interval.
=================================================
'''

# Import Libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from elasticsearch import Elasticsearch
import logging
import psycopg2 as db

# Function to fetch data from posgress
def QueryPostgreSql():
    """
    Fetches data from PostgreSQL and saves it to a CSV file.
    """
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn = db.connect(conn_string)
    df = pd.read_sql("SELECT * FROM table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_Alfarabi_data_raw.csv')
    print("-------Data Saved------")

# Data cleaning
def CleaningData():
    """
    Cleans data save to CSV
    """
    df = pd.read_csv('/opt/airflow/dags/P2M3_Alfarabi_data_raw.csv')
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    df.to_csv('/opt/airflow/dags/P2M3_Alfarabi_data_clean.csv')

# Transport data to Elasticsearch
def InsertElasticSearch():
    """
    Transports cleaned CSV data to Elasticsearch.
    """
    es = Elasticsearch('http://elasticsearch:9200')
    df = pd.read_csv('/opt/airflow/dags/P2M3_Alfarabi_data_clean.csv')
    for i, r in df.iterrows():
            doc = r.to_json()
            res = es.index(index="shopping_trends", doc_type="_doc", body=doc)
    print(res)

# Default arguments for the DAG
default_args = {
    'owner': 'Alfarabi',
    'start_date': datetime(2024, 5, 24),  # Adjusted start date
    'retry_delay': timedelta(minutes=2),
    'retries': 1,
}
    
# Define the DAG
with DAG(
    "customer_shopping_trends_dag",
    description='Customer Shopping Trends Data Cleaning and Analysis DAG',
    schedule_interval='30 6 * * *',  # Update schedule as needed
    default_args=default_args,
    catchup=False
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_data_from_postgresql',
        python_callable=QueryPostgreSql,
        dag=dag
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=CleaningData,
        dag=dag
    )

    insert_data = PythonOperator(
        task_id='insert_data_into_elasticsearch',
        python_callable=InsertElasticSearch,
        dag=dag
    )

# Define task dependencies
fetch_data >> clean_data >> insert_data
