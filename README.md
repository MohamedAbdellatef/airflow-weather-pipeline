# 🌤️ Airflow Weather Data Pipeline
This project implements a production-ready ETL pipeline using Apache Airflow, following the Medallion Architecture (Bronze → Silver). It extracts daily weather data from the OpenWeatherMap API, stores raw JSON in PostgreSQL (Bronze), transforms it into structured data (Silver), and makes it ready for analytics.

This project is designed to showcase **production-grade pipeline skills**:
- Airflow DAG orchestration with custom operators
- Layered data architecture (Bronze → Silver)
- Dockerized Airflow + PostgreSQL environment
- Industry-ready ETL best practices

---
## ✨ Features
- API Integration (OpenWeatherMap)
- Airflow DAG Orchestration (custom operators)
- Bronze → Silver Data Architecture
- PostgreSQL Storage 
- Dockerized Setup (Airflow + Postgres)
- Logging & Monitoring (via Airflow task logs, SLA, alerts)

  
## 🏗️ High-Level Architecture
The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) to ensure data quality and lineage. In this project we implement **Bronze** (raw JSON) and **Silver** (clean structured data).
![Data Architecture](docs/data_architecture.png)


## 🔄 Pipeline Workflow
The Airflow DAG defines the ETL orchestration:

![Pipeline Workflow](docs/pipeline_workflow.png)

### Task Breakdown
1. **create_table** → Initializes the Bronze table in PostgreSQL.  
2. **extract_weather** → Fetches daily weather JSON from API → Bronze.  
3. **create_silver_table** → Sets up schema for Silver table.  
4. **transform_weather** → Cleans & transforms Bronze data → Silver.  


## Tech Stack
<p>
<img src="https://img.shields.io/badge/Python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54"/>
<img src="https://img.shields.io/badge/postgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white"/>
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
│   └── requirements.txt  # Python dependencies
├── docker/
│   ├── .env.example                       # Example env vars (add API key here)
│   └── docker-compose.yml                 # Airflow + Postgres setup
├── sql/
│   ├── ddl_bronze_weather_data.sql        # Bronze table schema
│   └── ddl_silver_weather_data.sql        # Silver table schema
├── docs/
│   ├── data_architecture.png              # High-level architecture diagram
│   ├── pipeline_workflow.png              # Airflow DAG screenshot
│   └── data_model.drawio.png              # Data model (ERD)
├── tests/
│   └── test_weather_pipeline.py           # Example unit tests for operators
├── .gitignore
└── README.md
```
## 📊 Data Model
The pipeline uses a multi-layered data warehouse design:

- **Bronze Layer**: Stores raw JSON responses from the weather API.
- **Silver Layer**: Stores cleaned and structured data for analytics.

Below is the entity-relationship diagram:
![Data model](docs/data_model.drawio.png)

## ⚡ Setup Instructions
Follow these steps to run the project locally:

1. **Clone the repository**
   ```bash
   git clone https://github.com/<your-username>/airflow-weather-pipeline.git
   cd airflow-weather-pipeline
2. **Start services with Docker Compose**
   ```bash
     docker-compose up -d
3. **Initialize DB**
   ```bash
   docker exec -it airflow-webserver airflow db init ```
4. **Create user**
   ```bash
   docker exec -it airflow-webserver airflow users create \
    --username airflow \
    --firstname admin \
    --lastname user \
    --role Admin \
    --email admin@example.com \
    --password airflow
5. **Open Airflow UI**
   <p>
   -Open: http://localhost:8080
   -Username: airflow
   -Password: airflow
   </p>
7. **Enable DAG**
   -Navigate to the Airflow UI → DAGs tab.
   -Enable the weather_pipeline DAG.
   -Trigger it manually or wait for the schedule.
8. **Verify data in PostgreSQL**
   ```bash
     docker exec -it postgres psql -U airflow -d airflow
    \dt    -- list tables
    SELECT * FROM weather_silver LIMIT 5;



## 🚀 Future Improvements
- Add Gold Layer (aggregated weather metrics).  
- Deploy to cloud (AWS/GCP/Azure).  
- Add tests for data validation.  
- Add monitoring with Airflow SLA & alerts.  
