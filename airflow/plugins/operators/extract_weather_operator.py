from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json

class ExtractWeatherOperator(BaseOperator):
    """
    Custom Airflow operator to extract weather data from the OpenWeatherMap API
    and load it into a "Bronze" PostgreSQL table as raw JSON.

    This operator fetches the current weather for a specified city, and then
    ingests the raw JSON response into a specified database table.

    :param city: The city for which to fetch weather data.
    :type city: str
    :param openweathermap_conn_id: The Airflow connection ID for the
                                   OpenWeatherMap API. 
    :type openweathermap_conn_id: str
    :param postgres_conn_id: The Airflow connection ID for the PostgreSQL database.
    :type postgres_conn_id: str
    :param table_name: The name of the target table in the Bronze database.
                       This table should have a column of type JSONB to store the raw data.
    :type table_name: str
    """
    template_fields = ('city', 'table_name')

    @apply_defaults
    def __init__(self, city: str, openweathermap_conn_id: str, postgres_conn_id: str, table_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.city = city
        self.openweathermap_conn_id = openweathermap_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name

    def execute(self, context) -> dict:
        """
        Fetches weather data from the API, loads it into PostgreSQL,
        and returns the data. The returned dictionary is pushed to XComs.
        """
        # Use the BaseHook to retrieve the connection details securely
        connection = BaseHook.get_connection(self.openweathermap_conn_id)
        api_key = connection.password  # Get the API key from the password field
        url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}&appid={api_key}&units=metric"

        self.log.info(f"Requesting weather data for '{self.city}' from API...")
        # Make the API request
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            self.log.info(f"Successfully received data for '{self.city}'.")

        except requests.exceptions.RequestException as e:
            self.log.error(f"API request failed: {e}")
            # Re-raising the exception to fail the Airflow task
            raise

        # Connect to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        data_string = json.dumps(data)
        sql = f"INSERT INTO {self.table_name} (city, raw_json) VALUES (%s, %s)"
        
        self.log.info(f"Loading data into table: {self.table_name}")
        pg_hook.run(sql, parameters=(self.city, data_string))
        self.log.info("Data successfully loaded to Bronze table.")
        
        # The returned data will be pushed to XComs for any optional, immediate downstream tasks
        return data