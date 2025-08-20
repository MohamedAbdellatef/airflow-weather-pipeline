from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

class LoadWeatherOperator(BaseOperator):
    @apply_defaults
    def __init__(self, city: str, postgres_conn_id: str = "postgres_default", *args, **kwargs):
        super(LoadWeatherOperator, self).__init__(*args, **kwargs)
        self.city = city
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        weather_data = context['ti'].xcom_pull(task_ids='extract_weather')
        if not weather_data:
            self.log.error("No weather data found in XCom!")
            return

        # Connect to Postgres
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Make sure table exists (Bronze layer)
        create_sql = """
        CREATE TABLE IF NOT EXISTS bronze_weather (
            id SERIAL PRIMARY KEY,
            city TEXT,
            raw_json JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        pg_hook.run(create_sql)

        # Insert the raw JSON into Bronze table
        insert_sql = """
        INSERT INTO bronze_weather (city, raw_json)
        VALUES (%s, %s);
        """
        pg_hook.run(insert_sql, parameters=(self.city, json.dumps(weather_data)))

        self.log.info(f"Inserted weather data for {self.city} into Bronze table.")