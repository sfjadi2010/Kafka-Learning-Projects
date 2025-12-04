# Kafka Learning Project - Multi-Topic Architecture

This project demonstrates a complete Apache Kafka pipeline with dynamic topic creation, multi-topic data processing, and a modern tabbed UI interface. It uses Docker Compose to orchestrate Kafka, Zookeeper, FastAPI backends, and a React TypeScript frontend for CSV data streaming and visualization.

## Key Features

- **Dynamic Topic Creation**: Each uploaded CSV file automatically creates its own Kafka topic
- **Multi-Topic Architecture**: Separate database tables for each topic with independent data streams
- **Tabbed UI**: Modern React interface showing each topic in separate tabs with real-time updates
- **Real-time Processing**: Live consumer processing with auto-refresh capabilities
- **Type-Safe Development**: Full TypeScript support across the frontend
- **Drag-and-Drop Upload**: Intuitive file upload with visual feedback

## Components

- **Zookeeper**: Coordination service for Kafka (Port 2181)
- **Kafka Broker**: Message broker (Ports 9092 for external, 9093 for internal)
- **Kafka UI**: Web-based UI for managing Kafka (Port 8080)
- **Producer API**: FastAPI service to upload CSV files and create topics (Port 8000)
- **Consumer API**: FastAPI service to consume from all topics and store in topic-specific tables (Port 8001)
- **Producer Frontend**: Vite + React + TypeScript UI for CSV upload (Port 3000)
- **Consumer Frontend**: React UI with tabbed interface for viewing topic data (Port 5174)

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

- **Producer Frontend (Upload)**: <http://localhost:3000> - Modern UI with drag-and-drop CSV upload
- **Consumer Frontend (Tabbed UI)**: <http://localhost:5174> - View data by topic in separate tabs
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

Upload a CSV file to automatically create a topic and send data:

```bash
curl -X POST "http://localhost:8000/upload-csv" -F "file=@customers.csv"
```

**Response includes:**
- Topic name (based on filename)
- Whether topic was newly created
- Number of rows sent
- Sample data

**Topic Naming Convention:**
- `customers.csv` → `customers` topic → `topic_customers` table
- `products.csv` → `products` topic → `topic_products` table

### 2. Start Kafka Consumer

Start the consumer to read from all Kafka topics and store in topic-specific tables:

```bash
curl -X POST "http://localhost:8001/start-consumer"
```

The consumer subscribes to all topics using pattern matching and automatically:
- Creates a database table for each topic
- Registers topics in metadata table
- Stores data in topic-specific tables

### 3. List All Topics

Get all registered topics with metadata:

```bash
curl "http://localhost:8001/topics"
```

**Returns:**
- Topic name
- Table name
- Record count
- Created timestamp

### 4. Get Records for Specific Topic

Retrieve paginated records from a specific topic:

```bash
curl "http://localhost:8001/topics/customers/records?limit=10&offset=0"
```

### 5. Get Consumer Statistics

```bash
curl "http://localhost:8001/stats"
```

## API Endpoints

### Producer API (Port 8000)

- `POST /upload-csv` - Upload CSV file to Kafka (creates topic automatically)
- `GET /health` - Health check
- `GET /kafka-info` - Kafka connection info and topic list

### Consumer API (Port 8001)

- `POST /start-consumer` - Start consuming from all Kafka topics
- `POST /stop-consumer` - Stop the consumer
- `GET /topics` - Get list of all topics with metadata
- `GET /topics/{topic}/records` - Get records for specific topic (supports limit & offset)
- `GET /records` - Get all stored records (deprecated - use topic-specific endpoint)
- `GET /stats` - Get consumer statistics
- `DELETE /records` - Delete all records

## Frontend Features

### Producer Frontend (Port 3000)

The React TypeScript frontend provides a modern, user-friendly interface:

- **Drag-and-drop CSV upload** - Simply drag files or click to browse
- **Real-time health monitoring** - Live Kafka connection status indicator
- **Upload feedback** - Clear success/error messages with detailed information
- **Type-safe development** - Built with TypeScript for better code quality
- **Fast development** - Powered by Vite with Hot Module Replacement (HMR)

### Consumer Frontend (Port 5174)

Modern tabbed interface for viewing data by topic:

- **Topic Tabs** - Each Kafka topic displayed in separate tab
- **Record Counts** - Badge showing number of records per topic
- **Real-time Updates** - Auto-refresh every 5 seconds when consumer is active
- **Pagination** - Navigate through large datasets with customizable page size
- **Dynamic Columns** - Table columns automatically match CSV structure
- **Consumer Controls** - Start/stop consumer directly from UI
- **Timezone Handling** - Automatic UTC to local time conversion
- **Responsive Design** - Works on desktop and mobile devices

### Sample Data Files

The project includes ready-to-use sample CSV files:

- `customers.csv` - 30 customer records with contact information
- `products.csv` - 30 product records with categories and pricing
- `orders.csv` - 40 order records linking customers and products
- `data.csv` - Simple test data for quick testing

## Multi-Topic Architecture

### How It Works

1. **Upload**: User uploads CSV file via frontend (e.g., `customers.csv`)
2. **Topic Creation**: Producer creates Kafka topic `customers` if it doesn't exist
3. **Data Publishing**: Each CSV row sent as JSON message to topic
4. **Pattern Subscription**: Consumer subscribes to all topics using pattern `^(?!__).*`
5. **Dynamic Tables**: Consumer creates `topic_customers` table in SQLite
6. **Metadata Tracking**: Topic registered in `topics` metadata table
7. **Data Storage**: Messages stored in topic-specific table
8. **UI Display**: Frontend shows "customers" tab with record count

### Database Schema

**Metadata Table:**

```sql
CREATE TABLE topics (
    topic_name TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

**Topic-Specific Tables:**

```sql
CREATE TABLE topic_{name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_number INTEGER,
    filename TEXT,
    data TEXT,  -- JSON string
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

## Data Flow

1. **Upload**: User uploads CSV file via React frontend or directly to Producer API
2. **Parse & Send**: Producer API parses CSV and sends each row as JSON to topic (named after filename)
3. **Topic Creation**: Producer creates Kafka topic if it doesn't exist using KafkaAdminClient
4. **Consume**: Consumer API subscribes to all topics via pattern matching
5. **Table Creation**: Consumer creates topic-specific SQLite table on first message
6. **Store**: Data is stored in topic-specific table (e.g., `topic_customers`)
7. **Metadata**: Topic registered in `topics` table with record count
8. **Display**: Frontend shows tabs for each topic with live data updates
9. **Query**: Access data via topic-specific endpoints or directly in SQLite

## Technology Stack

### Infrastructure

- **Docker & Docker Compose** - Container orchestration
- **Apache Kafka 7.5.0** - Distributed event streaming platform
- **Zookeeper 7.5.0** - Kafka coordination service
- **Kafka UI** - Web-based Kafka management interface

### Backend

- **FastAPI 0.104.1** - Modern Python web framework with OpenAPI support
- **kafka-python 2.0.2** - Kafka client library with KafkaAdminClient
- **SQLite** - Lightweight embedded database with topic-specific tables
- **Python 3.11** - Programming language

### Frontend

- **React 18.2.0** - UI library with hooks
- **TypeScript 5.2.2** - Type-safe JavaScript
- **Vite 5.0.8** - Next-generation frontend tooling
- **Axios 1.6.2** - HTTP client for API communication
- **Node.js 20** - JavaScript runtime

## Troubleshooting

If you encounter issues:

1. Ensure Docker Desktop is running
2. Check if ports 2181, 3000, 5174, 8000, 8001, 8080, 9092, and 9093 are not in use
3. View logs for specific service: `docker compose logs -f [service-name]`
   - Services: `zookeeper`, `kafka`, `kafka-ui`, `producer-api`, `consumer-api`, `producer-frontend`, `consumer-frontend`
4. Restart services: `docker compose restart`
5. Rebuild containers: `docker compose up -d --build`
6. Check Kafka connection: Visit <http://localhost:8000/health>

### Common Issues

**Kafka cluster offline:**

- Ensure Zookeeper is running: `docker ps | grep zookeeper`
- Check Kafka logs: `docker logs kafka --tail 50`
- Version compatibility: Project uses Confluent 7.5.0 (Zookeeper mode)

**Frontend not loading:**

- Verify frontend containers are running: `docker ps | grep frontend`
- Check producer-frontend logs: `docker logs kafka-producer-frontend`
- Check consumer-frontend logs: `docker logs kafka-consumer-frontend`
- Ensure ports 3000 and 5174 are not in use

**CSV upload fails:**

- Check producer API health: <http://localhost:8000/health>
- Verify CORS is enabled in producer API
- Check file format (must be valid CSV)
- Ensure topic creation succeeded (check Kafka UI)

**Topics not appearing in Consumer Frontend:**

- Ensure consumer is started: Check status in UI or call `/start-consumer`
- Verify messages are in Kafka: Check Kafka UI at <http://localhost:8080>
- Check consumer logs for errors: `docker logs consumer-api --tail 50`
- Verify topic was created: Call `/topics` endpoint

**Data not showing in specific tab:**

- Click refresh button or wait for auto-refresh (5 seconds)
- Check if consumer processed messages: Look for logs with "Stored record from topic"
- Verify database table exists: `docker exec consumer-api ls /app/data/`

## Project Structure

```text
.
├── docker-compose.yml          # Orchestrates all services
├── producer/
│   ├── Dockerfile              # Producer API container
│   ├── app.py                  # FastAPI producer with topic creation
│   └── requirements.txt        # Python dependencies
├── consumer/
│   ├── Dockerfile              # Consumer API container
│   ├── app.py                  # FastAPI consumer with multi-topic support
│   ├── requirements.txt        # Python dependencies
│   └── data/                   # SQLite database directory
│       └── kafka_data.db       # SQLite database with topic tables
├── producer-frontend/
│   ├── Dockerfile              # Upload frontend container
│   ├── package.json            # Node dependencies
│   ├── vite.config.ts          # Vite configuration
│   ├── tsconfig.json           # TypeScript configuration
│   ├── index.html              # HTML entry point
│   └── src/
│       ├── main.tsx            # React entry point
│       ├── App.tsx             # Upload component
│       ├── App.css             # Styles
│       ├── types.ts            # TypeScript interfaces
│       └── vite-env.d.ts       # Vite type definitions
├── consumer-frontend/
│   ├── Dockerfile              # Data viewer frontend container
│   ├── package.json            # Node dependencies
│   ├── vite.config.ts          # Vite configuration
│   ├── tsconfig.json           # TypeScript configuration
│   ├── index.html              # HTML entry point
│   └── src/
│       ├── main.tsx            # React entry point
│       ├── App.tsx             # Tabbed viewer component
│       ├── App.css             # Styles with tab design
│       ├── types.ts            # TypeScript interfaces (Topic, Record, Stats)
│       └── vite-env.d.ts       # Vite type definitions
├── customers.csv               # Sample customer data
├── products.csv                # Sample product data
├── orders.csv                  # Sample order data
├── data.csv                    # Simple test data
└── README.md                   # This file
```

## Development

### Running Producer Frontend Locally

```bash
cd producer-frontend
npm install
npm run dev
```

The producer frontend will be available at <http://localhost:5173> (Vite's default port).

### Running Consumer Frontend Locally

```bash
cd consumer-frontend
npm install
npm run dev
```

The data viewer will be available at <http://localhost:5173>.

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

## Architecture Highlights

### Producer Features

- **KafkaAdminClient**: Manages topic lifecycle
- **Dynamic Topic Naming**: Converts filename to valid topic name (lowercase, underscores)
- **Topic Creation Check**: Creates topic only if it doesn't exist
- **Configurable Partitions**: Default 3 partitions, replication factor 1

### Consumer Features

- **Pattern Subscription**: Subscribes to all non-internal topics using regex `^(?!__).*`
- **Dynamic Table Creation**: Creates SQLite table for each new topic
- **Metadata Tracking**: Maintains `topics` table for topic discovery
- **Topic-Specific Storage**: Isolates data by topic in separate tables
- **JSON Data Storage**: Stores CSV data as JSON for flexible querying

### Frontend Architecture

- **Tab State Management**: Active tab persists across refreshes
- **Auto-Refresh**: Updates every 5 seconds when consumer is active
- **Pagination State**: Separate pagination for each topic
- **Dynamic Columns**: Table adapts to CSV structure
- **Gradient Tabs**: Active tab uses gradient matching app theme

## Use Cases

This project demonstrates patterns useful for:

- **Multi-tenant Data Processing**: Isolate data by customer/tenant in separate topics
- **File-based Streaming**: Convert batch files into streaming data
- **Data Lake Ingestion**: Route different data types to appropriate storage
- **Event-Driven ETL**: Transform and load data from various sources
- **Real-time Dashboards**: Display live data updates by category/topic

## License

This is a learning project for Apache Kafka and event-driven architecture.
