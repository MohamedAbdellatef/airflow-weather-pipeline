from airflow import DAG
from plugins.operators.extract_weather_operator import ExtractWeatherOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='weather_etl',
    description='ETL process for weather data from OpenWeatherMap API',
    start_date=datetime(2025, 8, 21),
    schedule_interval='@daily',
    retries=3,
    retry_delay=timedelta(minutes=15),
    catchup=False,
    tags=['weather', 'etl']
) as dag:

    # Task 1: Create the Bronze table if it doesn't exist.
    # This task uses the built-in PostgresOperator to execute an SQL file.
    create_bronze_table = PostgresOperator(
        task_id='create_bronze_table',
        postgres_conn_id='postgres_default',
        sql='sql/init_db.sql'
    )

    # Task 2: Extract data from the API and load it into the Bronze table.
    # This is my custom operator.
    Extract_weather_to_bronze_sql = ExtractWeatherOperator(
        task_id='extract_weather',
        city='Dubai',
        openweathermap_conn_id='openweathermap_default',
        postgres_conn_id='postgres_default',
        table_name='bronze_weather_data'
    )

    # Define task dependencies
    create_bronze_table >> Extract_weather_to_bronze_sql