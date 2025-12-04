from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import csv
import json
import io
from typing import List
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Kafka CSV Producer API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9093'

def get_kafka_admin():
    """Create and return a Kafka admin client"""
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            api_version=(2, 0, 2)
        )
        return admin
    except Exception as e:
        logger.error(f"Error creating Kafka admin client: {e}")
        raise

def create_topic_if_not_exists(topic_name: str):
    """Create a Kafka topic if it doesn't already exist"""
    try:
        admin = get_kafka_admin()
        
        # Check if topic exists
        existing_topics = admin.list_topics()
        
        if topic_name in existing_topics:
            logger.info(f"Topic '{topic_name}' already exists")
            admin.close()
            return False
        
        # Create new topic
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1
        )
        
        admin.create_topics([topic])
        logger.info(f"Created new topic: '{topic_name}'")
        admin.close()
        return True
        
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists")
        return False
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        raise

def get_topic_name_from_filename(filename: str) -> str:
    """Generate topic name from filename by removing .csv extension"""
    # Remove .csv extension and any path separators
    base_name = os.path.splitext(filename)[0]
    # Replace spaces and special characters with underscores
    topic_name = base_name.replace(' ', '_').replace('-', '_')
    # Convert to lowercase for consistency
    return topic_name.lower()

def get_kafka_producer():
    """Create and return a Kafka producer instance"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 0, 2)
        )
        return producer
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        raise

@app.get("/")
async def root():
    return {
        "message": "Kafka CSV Producer API",
        "endpoints": {
            "/upload-csv": "POST - Upload CSV file to Kafka",
            "/health": "GET - Health check"
        }
    }

@app.get("/health")
async def health_check():
    try:
        producer = get_kafka_producer()
        producer.close()
        return {"status": "healthy", "kafka": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """
    Upload a CSV file and send each row to Kafka.
    Creates a new topic based on filename if it doesn't exist.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    try:
        # Generate topic name from filename
        topic_name = get_topic_name_from_filename(file.filename)
        
        # Create topic if it doesn't exist
        topic_created = create_topic_if_not_exists(topic_name)
        
        # Read the CSV file
        contents = await file.read()
        csv_file = io.StringIO(contents.decode('utf-8'))
        csv_reader = csv.DictReader(csv_file)
        
        # Initialize Kafka producer
        producer = get_kafka_producer()
        
        # Send each row to Kafka
        row_count = 0
        rows_sent = []
        
        for row in csv_reader:
            # Add metadata
            message = {
                "row_number": row_count + 1,
                "filename": file.filename,
                "data": row
            }
            
            # Send to Kafka topic
            future = producer.send(topic_name, value=message)
            result = future.get(timeout=10)
            
            rows_sent.append(message)
            row_count += 1
            
            logger.info(f"Sent row {row_count} to Kafka topic '{topic_name}'")
        
        # Ensure all messages are sent
        producer.flush()
        producer.close()
        
        return {
            "status": "success",
            "filename": file.filename,
            "rows_sent": row_count,
            "topic": topic_name,
            "topic_created": topic_created,
            "sample_data": rows_sent[:5] if rows_sent else []
        }
    
    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing CSV: {str(e)}")

@app.get("/kafka-info")
async def kafka_info():
    """Get Kafka connection information"""
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "note": "Topics are created dynamically based on uploaded CSV filenames"
    }
