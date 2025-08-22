import pytest
import requests
import json
from dags.weather_pipeline import transform_weather_data

# -------------------
# 1. API Test
# -------------------
def test_openweather_api_connection():
    """Test OpenWeather API returns valid response."""
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": "Dubai", "appid": "demo"}  # replace 'demo' with your API key
    response = requests.get(url, params=params)

    assert response.status_code == 200
    data = response.json()
    assert "weather" in data
    assert "main" in data

# -------------------
# 2. Transformation Test
# -------------------
def test_transform_weather_data():
    """Test transformation produces correct schema."""
    # Fake Bronze data (sample API response)
    raw_data = {
        "main": {"temp": 305.15, "humidity": 60},
        "weather": [{"description": "clear sky"}],
        "dt": 1724210000,
        "name": "Dubai"
    }

    # Call transform function
    transformed = transform_weather_data(raw_data)

    # Validate schema
    assert isinstance(transformed, dict)
    assert "city" in transformed
    assert "temperature" in transformed
    assert "humidity" in transformed
    assert "description" in transformed
    assert "timestamp" in transformed
