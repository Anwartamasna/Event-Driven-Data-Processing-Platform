# Big Data Pipeline - User Event Processing

A production-style Docker-based big data pipeline that simulates user events, ingests them via Kafka, processes with Spark Streaming, aggregates with Spark Batch, and orchestrates with Airflow.

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Kafka     │     │  Kafka  │     │ Spark Streaming  │     │   Data Lake     │
│  Producer   │────▶│  Topic  │────▶│   (Continuous)   │────▶│  /data/raw/     │
│ (Events)    │     │         │     │                  │     │                 │
└─────────────┘     └─────────┘     └──────────────────┘     └────────┬────────┘
                                                                      │
                                                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Airflow DAG (Daily)                            │
│  ┌────────────────┐    ┌─────────────────┐    ┌──────────────────────┐      │
│  │ Wait for Data  │───▶│ Spark Batch Job │───▶│ Validate Results     │      │
│  │   (Sensor)     │    │  (Aggregation)  │    │   (Quality Check)    │      │
│  └────────────────┘    └─────────────────┘    └──────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                          ┌─────────────────┐
                          │   Data Lake     │
                          │ /data/processed/│
                          │ (Daily Metrics) │
                          └─────────────────┘
```

## 📁 Project Structure

```
bigdata-pipeline/
├── docker-compose.yml          # All services configuration
├── README.md                   # This file
│
├── airflow/
│   ├── Dockerfile              # Custom Airflow image with Spark
│   └── dags/
│       └── daily_batch_pipeline.py  # Airflow DAG
│
├── spark/
│   ├── streaming_job.py        # Spark Structured Streaming
│   └── batch_job.py            # Spark Batch Aggregation
│
├── kafka/
│   └── producer.py             # Event generator
│
└── data/
    ├── raw/                    # Raw events (from streaming)
    └── processed/              # Daily metrics (from batch)
```

## 🛠️ Tech Stack

| Component          | Tool                              |
|--------------------|-----------------------------------|
| Message Broker     | Apache Kafka (Confluent)          |
| Stream Processing  | Spark Structured Streaming 3.5    |
| Batch Processing   | Apache Spark 3.5                  |
| Orchestration      | Apache Airflow 2.8                |
| Storage            | Local Filesystem (Data Lake)      |
| Containers         | Docker + Docker Compose           |

## 🚀 Quick Start

### Step 1: Start All Services

```bash
cd bigdata-pipeline
docker-compose up -d
```

Wait for all services to be ready (about 1-2 minutes):

```bash
docker-compose ps
```

### Step 2: Initialize Airflow (First Time Only)

```bash
# Wait for airflow-init to complete
docker-compose logs airflow-init

# Access Airflow UI at http://localhost:8081
# Login: admin / admin
```

### Step 3: Create Kafka Topic

```bash
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic user_events \
  --partitions 3 \
  --replication-factor 1
```

### Step 4: Start Spark Streaming Job (Run Once)

This job runs continuously in the background:

```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /spark/streaming_job.py
```

> **Note:** Keep this terminal open. The streaming job writes data to `/data/raw/`.

### Step 5: Start Kafka Producer

In a **new terminal**, start the event producer:

```bash
# Option 1: Run inside Docker (requires Python in Kafka container)
docker exec -it kafka pip install kafka-python && \
docker exec -it kafka python /kafka/producer.py

# Option 2: Run locally (requires kafka-python installed)
cd bigdata-pipeline/kafka
pip install kafka-python
python producer.py --bootstrap-servers localhost:9092
```

### Step 6: Verify Data Flow

Check that raw data is being written:

```bash
ls -la data/raw/user_events/
```

### Step 7: Trigger Airflow DAG (Optional)

The DAG runs daily at 2 AM. To trigger manually:

1. Go to http://localhost:8081
2. Find `daily_batch_pipeline`
3. Toggle ON and click "Trigger DAG"

Or via CLI:

```bash
docker exec airflow-scheduler airflow dags trigger daily_batch_pipeline
```

## 📊 Sample Event Format

```json
{
  "user_id": 42,
  "event_type": "click",
  "timestamp": "2025-01-10T10:00:00",
  "page_id": 15,
  "session_id": "sess_12345"
}
```

**Event Types:**
- `click` (40%)
- `view` (35%)
- `purchase` (5%)
- `login` (10%)
- `logout` (10%)

## 🔍 Accessing Services

| Service          | URL                        | Credentials   |
|------------------|----------------------------|---------------|
| Airflow UI       | http://localhost:8081      | admin / admin |
| Spark Master UI  | http://localhost:8080      | -             |
| Kafka            | localhost:9092             | -             |

## 🧪 Testing the Pipeline

### Run Batch Job Manually

```bash
# Process today's date
docker exec spark-master spark-submit /spark/batch_job.py

# Process specific date
docker exec spark-master spark-submit /spark/batch_job.py --date 2025-01-15
```

### View Processed Metrics

```bash
ls -la data/processed/daily_metrics/
```

## 🛑 Shutdown

```bash
docker-compose down

# To remove volumes (data)
docker-compose down -v
```

## 📝 Interview Demo Points

✅ **Airflow UI** shows DAG success with green tasks  
✅ **Kafka topic** receives events in real-time  
✅ **Spark streaming** writes raw data continuously  
✅ **Batch job** produces aggregated daily metrics  
✅ **Production patterns**: sensor, validation, error handling  

## 🔧 Troubleshooting

### Kafka Connection Issues
```bash
# Check Kafka is running
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Spark Streaming Not Writing Data
```bash
# Check Spark logs
docker logs spark-master
```

### Airflow DAG Not Visible
```bash
# Restart scheduler
docker-compose restart airflow-scheduler
```

---

## License

MIT
