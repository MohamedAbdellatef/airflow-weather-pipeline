from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from plugins.operators.extract_weather_operator import ExtractWeatherOperator
from plugins.operators.transform_load_operator import TransformAndLoadSilverOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl",
    description="ETL process for weather data from OpenWeatherMap API",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["weather", "etl"],
    doc_md="""
    ### Weather ETL DAG  
    This pipeline extracts raw weather data from **OpenWeatherMap API** into a  
    Bronze Postgres table, then transforms & loads it into a Silver table.
    """,
) as dag:

    # Task 1: Create the Bronze table
    create_bronze_table = PostgresOperator(
        task_id="create_bronze_table",
        postgres_conn_id="postgres_default",
        sql="sql/ddl_bronze_weather_data.sql",
    )

    # Task 2: Create the Silver table
    create_silver_table = PostgresOperator(
        task_id="create_silver_table",
        postgres_conn_id="postgres_default",
        sql="sql/ddl_silver_weather_data.sql",
    )

    # Task 3: Extract data from API → Bronze
    extract_weather_to_bronze = ExtractWeatherOperator(
        task_id="extract_weather",
        city="Dubai",
        openweathermap_conn_id="openweathermap_default",
        postgres_conn_id="postgres_default",
        table_name="bronze_weather_data",
    )

    # Task 4: Transform data → Silver
    transform_weather_to_silver = TransformAndLoadSilverOperator(
        task_id="transform_weather",
        postgres_conn_id="postgres_default",
        silver_table_name="silver_weather_data",
        source_task_id="extract_weather",
    )
    # Define task dependencies
    [create_bronze_table, create_silver_table] >> Extract_weather_to_bronze_sql >> Transform_weather_to_silver_sql 
