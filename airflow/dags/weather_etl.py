from airflow import DAG
from plugins.operators.extract_weather_operator import ExtractWeatherOperator
from airflow.plugins.operators.transform_load_operator import TransformAndLoadSilverOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

"""
DAG for ETL process of weather data from OpenWeatherMap API.
Tasks:
1. Create bronze table for raw data.
2. Create silver table for transformed data.
3. Extract weather data and load into bronze table.
4. Transform raw data and load into silver table.
"""

with DAG(
    dag_id='weather_etl',
    description='ETL process for weather data from OpenWeatherMap API',
    start_date=datetime(2024,1,1),
    schedule_interval='@daily',
    execution_timeout=timedelta(minutes=5),
    retries=3,
    retry_delay=timedelta(minutes=15),
    catchup=False,
    tags=['weather', 'etl']
) as dag:

    # Task 1: Create the Bronze table if it doesn't exist.
    create_bronze_table = PostgresOperator(
        task_id='create_bronze_table',
        postgres_conn_id='postgres_default',
        sql='sql/ddl_bronze_weather_data.sql'
    )

    # Task 2: Create the Silver table if it doesn't exist.
    create_silver_table = PostgresOperator(
        task_id='create_silver_table',
        postgres_conn_id='postgres_default',
        sql='sql/ddl_silver_weather_data.sql' 
    )

    # Task 3: Extract data from the API and load it into the Bronze table.
    # This is my custom operator.
    Extract_weather_to_bronze_sql = ExtractWeatherOperator(
        task_id='extract_weather',
        city='Dubai',
        openweathermap_conn_id='openweathermap_default',
        postgres_conn_id='postgres_default',
        table_name='bronze_weather_data'
    )

    # Task 4: Transform raw data and load it into the Silver table.
    # This is my custom operator
    Transform_weather_to_silver_sql = TransformAndLoadSilverOperator(
        task_id='transform_weather',
        postgres_conn_id='postgres_default',
        silver_table_name='silver_weather_data',
        source_task_id='extract_weather'
    )
    # Define task dependencies
    [create_bronze_table, create_silver_table] >> Extract_weather_to_bronze_sql >> Transform_weather_to_silver_sql 