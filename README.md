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
The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) to ensure data quality and lineage. In this project we implement **Bronze** (raw JSON) and **Silver** (clean structured data).
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
│   ├── dags/
│   │   └── weather_etl.py                # Main DAG definition
│   ├── plugins/
│   │   └── operators/
│   │       ├── extract_weather_operator.py   # Extract & Load → Bronze
│   │       └── transform_load_operator.py    # Transform & Load → Silver
│   └── requirements.txt
├── docker/
│   ├── .env.example                       # Example env vars (add API key here)
│   └── docker-compose.yml                 # Airflow + Postgres setup
├── sql/
│   ├── ddl_bronze_weather_data.sql        # Bronze table schema
│   └── ddl_silver_weather_data.sql        # Silver table schema
├── docs/
│   ├── data_architecture.png              # High-level architecture diagram
│   └── data_model.drawio.png              # Data model (ERD)
├── .gitignore
└── README.md
```
## Data model
The pipeline uses a multi-layered data warehouse design:

- **Bronze Layer**: Stores raw JSON responses from the weather API.
- **Silver Layer**: Stores cleaned and structured data for analytics.

Below is the entity-relationship diagram:
![Data model](docs/data_model.drawio.png)

## ⚙️ Setup Instructions
1. Clone repo
2. Add API key in `.env`
3. Run `docker-compose up`
4. Access Airflow UI at `http://localhost:8080`

## 🚀 Future Improvements
- Add Gold Layer (aggregated weather metrics).  
- Deploy to cloud (AWS/GCP/Azure).  
- Add tests for data validation.  
- Add monitoring with Airflow SLA & alerts.  
