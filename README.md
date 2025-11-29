# Kafka Learning Project

This project sets up Apache Kafka with Zookeeper, Kafka UI, and two FastAPI services for CSV data processing using Docker Compose.

## Components

- **Zookeeper**: Coordination service for Kafka (Port 2181)
- **Kafka Broker**: Message broker (Ports 9092 for external, 9093 for internal)
- **Kafka UI**: Web-based UI for managing Kafka (Port 8080)
- **Producer API**: FastAPI service to upload CSV files to Kafka (Port 8000)
- **Consumer API**: FastAPI service to consume from Kafka and store in SQLite (Port 8001)
- **Frontend**: React UI for easy CSV upload (Port 3000)

## Getting Started

### Prerequisites
- Docker Desktop installed and running
- Docker Compose installed

### Starting the Services

```bash
docker compose up -d --build
```

### Accessing the Services

Once running, access:
- **React Frontend**: http://localhost:3000 (Main UI for uploading CSV)
- **Kafka UI**: http://localhost:8080
- **Producer API**: http://localhost:8000/docs (Swagger UI)
- **Consumer API**: http://localhost:8001/docs (Swagger UI)

### Stopping the Services

```bash
docker compose down
```

### Stopping and Removing Volumes

```bash
docker compose down -v
```

## Kafka Connection Details

- **External Connection** (from host machine): `localhost:9092`
- **Internal Connection** (between containers): `kafka:9093`
- **Zookeeper**: `localhost:2181`

## Using the APIs

### 1. Upload CSV to Kafka (Producer)

Upload a CSV file via the Producer API:

```bash
curl -X POST "http://localhost:8000/upload-csv" -F "file=@sample_data.csv"
```

Or use the Swagger UI at http://localhost:8000/docs

### 2. Start Kafka Consumer

Start the consumer to read from Kafka and store in SQLite:

```bash
curl -X POST "http://localhost:8001/start-consumer"
```

### 3. Get Stored Records

Retrieve records from SQLite database:

```bash
curl "http://localhost:8001/records?limit=10"
```

### 4. Get Consumer Statistics

```bash
curl "http://localhost:8001/stats"
```

### 5. Run the Test Script

A Python test script is included to test the complete flow:

```bash
pip install requests
python test_api.py
```

## API Endpoints

### Producer API (Port 8000)
- `POST /upload-csv` - Upload CSV file to Kafka
- `GET /health` - Health check
- `GET /kafka-info` - Kafka connection info

### Consumer API (Port 8001)
- `POST /start-consumer` - Start consuming from Kafka
- `POST /stop-consumer` - Stop the consumer
- `GET /records` - Get stored records (supports limit & offset)
- `GET /records/count` - Get total record count
- `GET /stats` - Get consumer statistics
- `DELETE /records` - Delete all records

## Data Flow

1. CSV file uploaded to Producer API
2. Producer sends each row to Kafka topic `csv-data`
3. Consumer reads from Kafka topic
4. Data stored in SQLite database (`consumer/data/kafka_data.db`)
5. Query the data via Consumer API

## Troubleshooting

If you encounter issues:
1. Ensure Docker Desktop is running
2. Check if ports 2181, 8080, 9092, and 9093 are not in use
3. View logs: `docker compose logs -f`
4. Restart services: `docker compose restart`
