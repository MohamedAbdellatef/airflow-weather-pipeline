-- This script initializes the bronze table for raw weather data.
-- A JSONB column is used for efficient querying of the raw JSON data.
CREATE TABLE IF NOT EXISTS bronze_weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    raw_json JSONB
);