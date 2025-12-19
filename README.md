# Dublin Bus Delay Prediction System

## Overview
This project implements an end to end machine learning system for predicting Dublin Bus arrival delays using real time GTFS data and weather information. The system ingests live streams, processes them through an Airflow ETL pipeline, trains ensemble models, and serves predictions through a FastAPI backend with a React based frontend.

High level pipeline flow  
Kafka (GTFS and Weather) → MongoDB → Airflow ETL → PostgreSQL → Model Training → FastAPI → React Frontend

---

## Repository Structure
```
├── Airflow/
│   ├── airflow/
│   │   ├── docker-compose.yaml
│   │   ├── airflow.cfg
│   │   └── requirements.txt
│   ├── dags/
│   │   ├── dublin_bus_etl_optimised.py
│   │   ├── dublin_bus_ml_training_rf_bagged.py
│   │   └── ml_pipeline_catboost_bagging.py
│   └── artifacts/
│
├── Kafka/
│   ├── Kafka_Producer/
│   ├── Kafka_Consumer/
│   └── Kafka_Docker_Setup/
│
├── Database/
│   └── postgresdocker/
│
├── Backend/
│   └── app.py
│
├── Frontend/
│   └── dublin_bus_frontend_1/
│
├── Models/
├── Visulazation/
└── README.md
```

---

## Data Sources
The system uses live public APIs and does not rely on static datasets.

GTFS Real Time Data from Transport for Ireland  
Vehicle Positions  
Trip Updates  

Weather Data  
Open Meteo API providing temperature, humidity, wind speed and precipitation

---

## Kafka Streaming Layer

### Producers
vehicle_position_producer.py  
Fetches live GPS positions every 30 seconds

trip_updates_producer.py  
Fetches stop level arrival and departure delays every 10 seconds

weather_producer.py  
Fetches Dublin weather data every 5 minutes

### Consumer
gfts_realtime_consumer.py  
Consumes all Kafka topics and stores raw JSON data into MongoDB collections

---

## MongoDB Raw Storage
MongoDB acts as a raw buffer layer before ETL processing.

Database  
dublin_bus_db

Collections  
vehicles_raw  
trip_updates_raw  
weather_raw  

---

## Airflow ETL Pipeline

Main DAG  
dublin_bus_etl_optimised.py

Runs every 5 minutes and performs  
Table creation  
Incremental extraction from MongoDB  
Data cleaning and validation  
Feature engineering  
Weather and bus data merge  
Load into PostgreSQL  
ETL state tracking  

Final dataset  
bus_weather_merged

---

## PostgreSQL Database

Used for cleaned data and model training.

Key table  
bus_weather_merged

Important columns  
route_id  
vehicle_id  
stop_id  
stop_sequence  
latitude  
longitude  
arrival_delay  
is_late  
temperature  
humidity  
wind_speed  
precipitation  
temperature_category  
weather_severity  

---

## Model Training

### Random Forest Ensemble
DAG  
dublin_bus_ml_training_rf_bagged.py

Trains multiple Random Forest models using bagging  
Evaluates using R2 MAE and RMSE  
Registers metrics in model_registry  

### CatBoost Ensemble
DAG  
ml_pipeline_catboost_bagging.py

Trains multiple CatBoostRegressor models  
Uses early stopping  
Registers metrics in model_registry  

---

## FastAPI Backend

Main file  
Backend/app.py

Key endpoints  
GET /health  
GET /routes  
GET /predict/latest  
GET /predict/route/{route}  
GET /model/performance  

Loads the latest registered model automatically.

---

## Frontend Application
Built using React and TypeScript.

Pages  
Home  
Live Map  
Predictions  
Stop Arrivals  
Vehicle Tracking  
Analytics  

Runs on http://localhost:3000

---

## Visualization Module

File  
Visulazation/visulation.py

Generates  
Delay distributions  
Correlation heatmaps  
Interactive maps  
Animated delay GIFs  

Outputs stored in  
Plots/visualizations  
Plots/videos  

---

## Setup Instructions

Prerequisites  
Python 3.8 or higher  
Node.js  
Docker  

Start PostgreSQL  
docker-compose up -d

Start Kafka  
docker-compose up -d

Start MongoDB  
docker run -d -p 27017:27017 mongo

Run Airflow  
docker-compose up -d

Run Backend  
uvicorn app:app --port 8888

Run Frontend  
npm install  
npm start  

---

## Results
Typical performance  
R2 around 0.8  
MAE around 20 to 25 seconds  
RMSE around 40 to 50 seconds  

Visualizations highlight delay patterns across routes time of day and weather.

---

## Authorship
Developed as a university project by the Dublin Bus Data Science Team 2025
