CREATE TABLE IF NOT EXISTS bronze_weather (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);