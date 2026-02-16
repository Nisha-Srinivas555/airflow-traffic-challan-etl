from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

# Default arguments
default_args = {
    'owner': 'nisha',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# -------------------
# TASK FUNCTIONS
# -------------------

def extract():
    df = pd.read_csv('/opt/airflow/data/echallan_daily_data.csv')
    df.to_csv('/opt/airflow/data/extracted.csv', index=False)

def transform():
    df = pd.read_csv('/opt/airflow/data/extracted.csv')

    # Convert date column
    df['date'] = pd.to_datetime(df['date'])

    # Date features
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['day_of_week'] = df['date'].dt.day_name()
    df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])

    # Derived KPIs
    df['disposal_rate'] = (df['disposedChallan'] / df['totalChallan']).round(2)
    df['pending_rate'] = (df['pendingChallan'] / df['totalChallan']).round(2)
    df['avg_amount_per_challan'] = (df['totalAmount'] / df['totalChallan']).round(2)
    df['court_case_ratio'] = (df['totalCourt'] / df['totalChallan']).round(2)

    # Data Quality
    if df.isnull().values.any():
        raise ValueError("Null values detected!")


    if (df['totalAmount'] < 0).any():
        raise ValueError("Negative total amount detected!")

    df.to_csv('/opt/airflow/data/transformed.csv', index=False)

def load():
    df = pd.read_csv('/opt/airflow/data/transformed.csv')

    engine = create_engine("postgresql://airflow:airflow@postgres/airflow")

    df.to_sql(
        name="traffic_challan_analytics",
        con=engine,
        if_exists="replace",
        index=False
    )

# -------------------
# DAG DEFINITION
# -------------------

with DAG(
    dag_id='traffic_challan_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['traffic_challan', 'etl']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load
    )

    extract_task >> transform_task >> load_task






