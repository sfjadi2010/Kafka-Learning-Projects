# Kafka Learning Project

This project sets up Apache Kafka with Zookeeper, Kafka UI, FastAPI backend services, and a modern React TypeScript frontend for CSV data processing using Docker Compose.

## Components

- **Zookeeper**: Coordination service for Kafka (Port 2181)
- **Kafka Broker**: Message broker (Ports 9092 for external, 9093 for internal)
- **Kafka UI**: Web-based UI for managing Kafka (Port 8080)
- **Producer API**: FastAPI service to upload CSV files to Kafka (Port 8000)
- **Consumer API**: FastAPI service to consume from Kafka and store in SQLite (Port 8001)
- **Frontend**: Vite + React + TypeScript UI for CSV upload with drag-and-drop support (Port 3000)

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

- **React TypeScript Frontend**: <http://localhost:3000> - Modern UI with drag-and-drop CSV upload
- **Kafka UI**: <http://localhost:8080> - Kafka cluster management
- **Producer API**: <http://localhost:8000/docs> - Swagger UI for CSV upload API
- **Consumer API**: <http://localhost:8001/docs> - Swagger UI for data retrieval

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

Or use the Swagger UI at <http://localhost:8000/docs>

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

## Frontend Features

The React TypeScript frontend provides a modern, user-friendly interface:

- **Drag-and-drop CSV upload** - Simply drag files or click to browse
- **Real-time health monitoring** - Live Kafka connection status indicator
- **Type-safe development** - Built with TypeScript for better code quality
- **Fast development** - Powered by Vite with Hot Module Replacement (HMR)
- **Responsive design** - Works on desktop and mobile devices
- **Upload feedback** - Clear success/error messages with detailed information

### Sample Data Files

The project includes ready-to-use sample CSV files:

- `sample_customers.csv` - 30 customer records
- `sample_products.csv` - 30 product records with categories
- `sample_orders.csv` - 40 order records linking customers and products

## Data Flow

1. **Upload**: User uploads CSV file via React frontend or directly to Producer API
2. **Parse & Send**: Producer API parses CSV and sends each row to Kafka topic `csv-data`
3. **Consume**: Consumer API reads messages from Kafka topic in real-time
4. **Store**: Data is stored in SQLite database (`consumer/data/kafka_data.db`)
5. **Query**: Access stored data via Consumer API endpoints or SQLite directly

## Technology Stack

### Infrastructure

- **Docker & Docker Compose** - Container orchestration
- **Apache Kafka 7.5.0** - Distributed event streaming platform
- **Zookeeper 7.5.0** - Kafka coordination service
- **Kafka UI** - Web-based Kafka management interface

### Backend

- **FastAPI 0.104.1** - Modern Python web framework
- **kafka-python 2.0.2** - Kafka client library
- **SQLite** - Lightweight embedded database
- **Python 3.11** - Programming language

### Frontend

- **React 18.2.0** - UI library
- **TypeScript 5.2.2** - Type-safe JavaScript
- **Vite** - Next-generation frontend tooling
- **Axios 1.6.2** - HTTP client
- **Node.js 18** - JavaScript runtime

## Troubleshooting

If you encounter issues:

1. Ensure Docker Desktop is running
2. Check if ports 2181, 3000, 8000, 8001, 8080, 9092, and 9093 are not in use
3. View logs for specific service: `docker compose logs -f [service-name]`
   - Services: `zookeeper`, `kafka`, `kafka-ui`, `producer-api`, `consumer-api`, `frontend`
4. Restart services: `docker compose restart`
5. Rebuild containers: `docker compose up -d --build`
6. Check Kafka connection: Visit <http://localhost:8000/health>

### Common Issues

**Kafka cluster offline:**

- Ensure Zookeeper is running: `docker ps | grep zookeeper`
- Check Kafka logs: `docker logs kafka --tail 50`
- Version compatibility: Project uses Confluent 7.5.0 (Zookeeper mode)

**Frontend not loading:**

- Verify frontend container is running: `docker ps | grep frontend`
- Check frontend logs: `docker logs kafka-frontend`
- Ensure port 3000 is not in use

**CSV upload fails:**

- Check producer API health: <http://localhost:8000/health>
- Verify CORS is enabled in producer API
- Check file format (must be valid CSV)

## Project Structure

```text
.
├── docker-compose.yml          # Orchestrates all services
├── producer/
│   ├── Dockerfile              # Producer API container
│   ├── app.py                  # FastAPI producer application
│   └── requirements.txt        # Python dependencies
├── consumer/
│   ├── Dockerfile              # Consumer API container
│   ├── app.py                  # FastAPI consumer application
│   ├── requirements.txt        # Python dependencies
│   └── data/                   # SQLite database directory
│       └── kafka_data.db       # SQLite database file
├── frontend/
│   ├── Dockerfile              # Frontend container
│   ├── package.json            # Node dependencies
│   ├── vite.config.ts          # Vite configuration
│   ├── tsconfig.json           # TypeScript configuration
│   ├── index.html              # HTML entry point
│   └── src/
│       ├── main.tsx            # React entry point
│       ├── App.tsx             # Main component
│       ├── App.css             # Component styles
│       ├── types.ts            # TypeScript interfaces
│       └── vite-env.d.ts       # Vite type definitions
├── sample_customers.csv        # Sample customer data
├── sample_products.csv         # Sample product data
├── sample_orders.csv           # Sample order data
└── README.md                   # This file
```

## Development

### Running Frontend Locally

```bash
cd frontend
npm install
npm run dev
```

The frontend will be available at <http://localhost:5173> (Vite's default port).

### Running APIs Locally

**Producer API:**

```bash
cd producer
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

**Consumer API:**

```bash
cd consumer
pip install -r requirements.txt
uvicorn app:app --reload --port 8001
```

## License

This is a learning project for Apache Kafka and event-driven architecture.
