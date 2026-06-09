# Real-Time Traffic Congestion Prediction System

An end-to-end real-time traffic monitoring, data streaming, and machine-learning-driven congestion prediction system. The application ingests live spatial traffic flow data from the TomTom API, streams it through Apache Kafka, processes it in real time using Apache Spark (ETL Medallion architecture), stores it as Parquet data lakes, runs prediction inference using Deep Learning (LSTM & GRU) or Machine Learning (Random Forest) models, and exposes these predictions via a FastAPI backend to an interactive, real-time web dashboard.

---

## 🏗️ System Architecture

The project implements a modern real-time data streaming pipeline and ML deployment architecture:

```mermaid
graph TD
    %% Data Sources
    A1[TomTom API Async Pollers] -->|Raw Traffic Flow JSON| B[Apache Kafka Broker]
    A2[Bangalore Traffic Simulator] -->|Raw Simulated JSON| B
    
    %% Message Broker
    subgraph Message Broker [Apache Kafka Infrastructure]
        B[Kafka Topic: traffic.raw]
    end

    %% Streaming Processing
    subgraph Streaming ETL [Apache Spark / PySpark Engine]
        C[Spark Streaming Job: Silver Layer] -->|Reads from Kafka| B
        C -->|Validates & Cleans Data| D[(Parquet Silver Table)]
        E[Spark Aggregation Job: Gold Layer] -->|Reads Silver Parquet| D
        E -->|30s Window Aggregation| F[(Parquet Gold Table)]
    end

    %% Backend and ML
    subgraph API & Inference Service [FastAPI Web Application]
        G[FastAPI Backend] -->|Polls Latest Data| F
        H[TensorFlow Keras / Joblib Models] -->|Real-Time Inference| G
        G -->|Loads Scalers / Model Weights| I[LSTM / GRU / RF Models]
    end

    %% Clients
    subgraph Visual Analytics [Frontend Client Interface]
        J[Web Dashboard index.html] -->|REST API Requests| G
        G -->|WebSockets Live Broadcast| J
        J -->|Visualizes Charts / Predictions| K[Chart.js / Dynamic UI]
    end
    
    %% Styles
    style Message Broker fill:#1e1e24,stroke:#3c4043,color:#e8eaed
    style Streaming ETL fill:#2d3037,stroke:#5f6368,color:#e8eaed
    style API & Inference Service fill:#243447,stroke:#8ab4f8,color:#e8eaed
    style Visual Analytics fill:#202e3b,stroke:#81c995,color:#e8eaed
```

---

## ✨ Key Features

1. **Dual Data Ingestion Channels**:
   - **TomTom Producer**: An asynchronous poll-based ingestor utilizing `aiohttp` and `asyncio` to extract live congestion data from the TomTom Traffic Flow APIs for high-traffic corridors in Bangalore.
   - **Simulated Producer**: A configurable mock engine generating traffic flow parameters (speeds, travels times, congestion indexes) using statistical variations to test pipeline resilience under peak conditions.
2. **Robust Streaming Infrastructure**:
   - Decoupled event storage with Apache Kafka and Apache Zookeeper.
   - Resilience against network failures via exponential backoffs and transaction checks.
3. **Structured Medallion Storage**:
   - **Silver Layer**: Cleanses raw ingestion events, computes congestion indices where missing, normalizes values between 0.0 and 1.0, and appends records as partitioned Parquet tables.
   - **Gold Layer**: Uses PySpark streaming aggregation over a time-window (e.g., 30 seconds) to compute road statistics like running averages, speed variations, and rolling traffic trends.
4. **Multi-Model Inference Engine**:
   - Real-time prediction of road congestion index in the next 5 minutes.
   - Supported models:
     - Long Short-Term Memory (LSTM) network.
     - Gated Recurrent Unit (GRU) network.
     - Hybrid models (LSTM-GRU & GRU-LSTM configurations).
     - Scikit-Learn Random Forest regressor.
5. **FastAPI Web Service**:
   - Serves current metrics, historical charts, and invokes machine learning predictions asynchronously.
   - Utilizes persistent WebSockets to push live data events to connected dashboards every 5 seconds.
6. **Premium Visual Interface**:
   - Dark theme designed around material aesthetics.
   - Real-time numerical value counters, trend symbols, and historical/prediction plotting using Chart.js.

---

## 📁 Project Structure

```
Real Time Traffic Prediction/
├── docker-compose.yml       # Docker configuration for Kafka and Zookeeper
├── requirements.txt         # Python package dependencies
├── .env.example             # Configuration template for local API keys
├── .gitignore               # Excludes python cache, envs, IDEs, and heavy data layers
├── reports/                 # Project documentation, synopsis, certificates, and base paper
│   ├── Synopsis_MajorProject_Traffic_congestionprediction.pdf
│   ├── TCP Report_merged.pdf
│   └── Major_p2.pptx
├── notebooks/               # Model training and data exploration sandbox
│   ├── Realtime_Traffic_Predictor.ipynb
│   └── plots/               # Saved figures and training performance graphs
├── data/                    # Local medallion data lake (ignored by git)
│   ├── silver/              # Parsed and structured raw events
│   └── gold/                # Time-window aggregated analytical metrics
└── src/                     # Source code directory
    ├── ingestion/           # Ingestors / Producers sending events to Kafka
    │   ├── producer.py          # Simulated traffic producer
    │   └── tomtom_producer.py   # Live TomTom API integration producer
    ├── streaming/           # PySpark batching and streaming pipelines
    │   ├── spark_job.py         # Ingestion -> Silver Parquet pipeline
    │   └── spark_gold.py        # Silver -> Gold windowed aggregation pipeline
    ├── models/              # Saved model weights (.keras) and feature scalers (.joblib)
    │   ├── scaler.joblib        # Scikit-Learn MinMaxScaler for feature engineering
    │   ├── lstm.keras           # Trained LSTM Model
    │   ├── gru.keras            # Trained GRU Model
    │   └── random_forest.joblib # Trained Random Forest Model
    └── dashboard/           # API and visual representation layers
        ├── fastapi_backend.py   # REST API server & WebSocket dispatcher
        └── frontend/            # Client interface
            └── index.html       # Web dashboard with interactive Chart.js graphs
```

---

## 🛠️ Installation & Setup

### Prerequisites

- **Python**: `python 3.11.x` is recommended.
- **Java**: Java JDK 8 or 11 installed (Required by Apache Spark). Set the `JAVA_HOME` environment variable.
- **Hadoop Binaries (Windows)**:
  - Download `winutils.exe` and `hadoop.dll` for Hadoop version 3.3.x (or matching your Spark installation).
  - Place them in `C:\hadoop\bin`.
  - Add `HADOOP_HOME="C:\hadoop"` to system variables, and append `%HADOOP_HOME%\bin` to your system `PATH`.
- **Docker & Docker Compose**: Needed to run Zookeeper and Apache Kafka services locally.

### Step 1: Install Python Dependencies

Clone the project, create a Python virtual environment, and install dependencies:

```bash
# Initialize virtual environment
python -m venv venv

# Activate virtual environment
# On Windows (PowerShell):
.\venv\Scripts\Activate.ps1
# On macOS/Linux:
source venv/bin/activate

# Install required libraries
pip install -r requirements.txt
```

### Step 2: Configure Environment Variables

1. Copy [.env.example](.env.example) to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Open the newly created `.env` file and insert your API keys:
   - **`TOMTOM_API_KEY`**: Acquire one from the [TomTom Developer Portal](https://developer.tomtom.com/).
   - **`OPENWEATHER_API_KEY`**: (Optional) For weather-affected predictions.

---

## 🚀 Execution Guide

Follow these steps in separate terminals (ensure your virtual environment is active in each):

### 1. Launch Kafka Broker & Zookeeper

```bash
docker-compose up -d
```
*Verify containers are running via `docker ps`.*

### 2. Run Data Ingestion

Choose either live TomTom polling or the Bangalore traffic simulator:

**Live TomTom Data Ingestion:**
```bash
python src/ingestion/tomtom_producer.py
```

**Simulated Data Ingestion:**
```bash
python src/ingestion/producer.py
```

### 3. Start PySpark Streaming ETL Jobs

To build the medallion tables, start both the Silver layer parser and Gold layer aggregator:

**Start Silver ETL pipeline (Kafka to Silver Parquet):**
```bash
python src/streaming/spark_job.py
```

**Start Gold ETL pipeline (Silver to Gold Window Aggregations):**
```bash
python src/streaming/spark_gold.py
```
*As Spark processes streams, check the generated parquet files under `data/silver/` and `data/gold/`.*

### 4. Boot FastAPI Backend Server

```bash
uvicorn src.dashboard.fastapi_backend:app --reload
```
*The API will start at `http://localhost:8000`. You can explore interactive API documentation at `http://localhost:8000/docs`.*

### 5. Access Dashboard

Simply open the file [src/dashboard/frontend/index.html](src/dashboard/frontend/index.html) directly in any modern web browser or host it with a simple web server.

---

## 📡 API Reference

### REST Endpoints

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/` | `GET` | API details, status metadata, and active routing definitions. |
| `/health` | `GET` | Service health, list of loaded AI models, and scaler load status. |
| `/api/roads` | `GET` | Returns list of unique road IDs stored in the Gold Parquet files. |
| `/api/current/{road_id}` | `GET` | Fetches the most recent Speed, Congestion Index, Congestion Level, and calculated Traffic Trend for a road. |
| `/api/historical/{road_id}` | `GET` | Returns historical speed/congestion data points. Parameters: `hours` (default: 2). |
| `/api/predict` | `POST` | Generates a 5-minute congestion index prediction. Takes a JSON request. |

#### Predict JSON Request Schema:
```json
{
  "road_id": "blr_silkboard",
  "model_name": "LSTM"
}
```
*Supported `model_name` values: `LSTM`, `GRU`, `LSTM_GRU`, `GRU_LSTM`, `Random_Forest`*

#### Predict JSON Response Schema:
```json
{
  "road_id": "blr_silkboard",
  "model": "LSTM",
  "predicted_timestamp": "2026-06-09T18:20:00.000Z",
  "predicted_congestion_index": 0.428,
  "current_congestion_index": 0.385,
  "current_speed": 38.4,
  "last_update": "2026-06-09T18:15:00.000Z",
  "confidence_score": 0.92
}
```

### WebSocket Streaming

- **Endpoint**: `ws://localhost:8000/ws`
- **Output Frequency**: Broadcasts live updates for all roads every 5 seconds.
- **Message format**:
  ```json
  {
    "type": "update",
    "data": [
      {
        "road_id": "blr_silkboard",
        "timestamp": "2026-06-09T18:15:00.000Z",
        "avg_speed": 38.4,
        "avg_congestion_index": 0.385,
        "congestion_level": "Medium"
      }
    ]
  }
  ```

---

## 🧠 Model Training & Engineering

The deep learning and machine learning models are designed to use sequence-based historical lookbacks to project congestion index values.

- **Lookback Window**: Set to `6` intervals (corresponding to 30 minutes of historical data when sampled in 5-minute segments).
- **Features Engineered**:
  - `avg_speed` and `avg_congestion_index` (current values)
  - Date-Time Features: `hour`, `minute`, `day_of_week`, `is_weekend`, `is_rush_hour`
  - Cyclical Time Encodings: `time_sin`, `time_cos`, `day_sin`, `day_cos` (preserves hour/day continuity)
  - Statistical Changes: `speed_change`, `congestion_change` (difference from prior step)
  - Rolling Stats: `speed_rolling_mean_3`, `speed_rolling_std_3`, `congestion_rolling_mean_6`, etc.
- **Training Pipeline**: Fully documented inside the [notebooks/Realtime_Traffic_Predictor.ipynb](notebooks/Realtime_Traffic_Predictor.ipynb). Run this notebook to retrain the models and save scaled parameters (`scaler.joblib`) and neural networks weights (`.keras` files) directly to `src/models/`.