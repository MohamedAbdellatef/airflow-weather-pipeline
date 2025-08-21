# 🌤️ Airflow Weather Data Pipeline
This project demonstrates a **data engineering ETL pipeline** built with **Apache Airflow**.  
It extracts daily weather data from the [OpenWeatherMap API](https://openweathermap.org/api),  
loads raw JSON into a **Bronze layer** in PostgreSQL, transforms it into a structured format,  
and stores the results in a **Silver layer** for analytics.

This project is designed to showcase **production-grade pipeline skills**:
- Airflow DAG orchestration with custom operators
- Layered data architecture (Bronze → Silver)
- Dockerized Airflow + PostgreSQL environment
- Industry-ready ETL best practices

---
## 🏗️ High-Level Architecture
The pipeline follows the Medallion Architecture to ensure data quality and lineage, progressively refining data from its raw state to a clean, query-ready format.
![Data Architecture](docs/data_architecture.png)

## Tech Stack
<p>
<img src="https://img.shields.io/badge/Python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54"/>
<img src="https://img.shields.io/badge/SQL-336791?style=for-the-badge&logo=postgresql&logoColor=white"/>
<img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white"/>
<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
</p>

## 📂 Project Structure
```
airflow-weather-pipeline-project/
├── airflow/
│ ├── dags/
│ │ └── weather_etl.py # Main DAG
│ ├── plugins/
│ │ └── operators/
│ │ ├── extract_weather_operator.py # Extract & Load → Bronze
│ │ └── transform_load_operator.py # Transform & Load → Silver
│ └── requirements.txt
├── docker/
│ ├── .env.example
│ └── docker-compose.yml # Airflow + Postgres setup
├── sql/
│ ├── ddl_bronze_weather_data.sql # Bronze table schema
│ └── ddl_silver_weather_data.sql # Silver table schema
├── .gitignore
└── README.md
```
## ⚙️ Setup Instructions
1. Clone repo
2. Add API key in `.env`
3. Run `docker-compose up`
4. Access Airflow UI at `http://localhost:8080`
