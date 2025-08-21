-- This script creates the silver table for transformed, clean weather data.
-- This table is designed for easy querying and analysis.

CREATE TABLE IF NOT EXISTS silver_weather_data (
	id SERIAL PRIMARY KEY,
	city VARCHAR(50),
	country VARCHAR(30),
	longitude FLOAT,
	latitude FLOAT,
	observed_at TIMESTAMP WITH TIME ZONE,
	temperature_celsius FLOAT,
	humidity_percent INTEGER,
	pressure_hpa INTEGER,
	wind_speed_mps FLOAT,
	weather_description VARCHAR(100),	
	sunrise_at TIMESTAMP WITH TIME ZONE,
	sunset_at TIMESTAMP WITH TIME ZONE,
	processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
