import json
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class TransformAndLoadSilverOperator(BaseOperator):
    """
    Custom Airflow operator to transform raw weather data from XComs
    and load it into the Silver PostgreSQL table.

    This operator pulls the raw JSON data pushed by an upstream task,
    transforms it into a structured format, and inserts it into the final
    analytics-ready table.

    :param postgres_conn_id: The Airflow connection ID for the PostgreSQL database.
    :type postgres_conn_id: str
    :param silver_table_name: The name of the target Silver table.
    :type silver_table_name: str
    :param source_task_id: The task_id of the upstream task that provides the
                           raw data via XComs.
    :type source_task_id: str
    """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id: str,
                 silver_table_name: str,
                 source_task_id: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.silver_table_name = silver_table_name
        self.source_task_id = source_task_id

    def execute(self, context):
        # Step 1: Pull the raw data from XComs
        ti = context['ti']
        self.log.info(f"Pulling data from upstream task: {self.source_task_id}")
        json_data = ti.xcom_pull(task_ids=self.source_task_id)
        
        if not json_data:
            raise ValueError("No data received from XComs. Upstream task may have failed.")
        
        # Step 2: Transform the raw JSON data into a structured format
        self.log.info("Transforming raw data...")

        transformed_data = {
            "city": json_data.get("name"),
            "country": json_data.get("sys", {}).get("country"),
            "longitude": json_data.get("coord", {}).get("lon"),
            "latitude": json_data.get("coord", {}).get("lat"),
            "observed_at": datetime.fromtimestamp(json_data.get("dt", 0)),
            "temperature_celsius": json_data.get("main", {}).get("temp"),
            "humidity_percent": json_data.get("main", {}).get("humidity"),
            "pressure_hpa": json_data.get("main", {}).get("pressure"),
            "wind_speed_mps": json_data.get("wind", {}).get("speed"),
            "weather_description": json_data.get("weather", [{}])[0].get("description"),
            "sunrise_at": datetime.fromtimestamp(json_data.get("sys", {}).get("sunrise", 0)),
            "sunset_at": datetime.fromtimestamp(json_data.get("sys", {}).get("sunset", 0)),
        }

        self.log.info(f"Transformed data: {transformed_data}")
        
        # Step 3: Load the transformed data into the Silver table
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        # The PostgresHook's insert_rows method is efficient for this.
        # We get the column names and values from our transformed dictionary.
        columns = transformed_data.keys()
        values = [tuple(transformed_data.values())] # insert_rows expects a list of tuples

        self.log.info(f"Inserting transformed data into {self.silver_table_name}...")
        pg_hook.insert_rows(
            table=self.silver_table_name,
            rows=values,
            target_fields=columns
        )
        self.log.info("Successfully loaded data into the Silver table.")