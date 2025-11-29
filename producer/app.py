from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
import csv
import json
import io
from typing import List
import logging

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
KAFKA_TOPIC = 'csv-data'

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
    Upload a CSV file and send each row to Kafka
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    try:
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
            
            # Send to Kafka
            future = producer.send(KAFKA_TOPIC, value=message)
            result = future.get(timeout=10)
            
            rows_sent.append(message)
            row_count += 1
            
            logger.info(f"Sent row {row_count} to Kafka")
        
        # Ensure all messages are sent
        producer.flush()
        producer.close()
        
        return {
            "status": "success",
            "filename": file.filename,
            "rows_sent": row_count,
            "topic": KAFKA_TOPIC,
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
        "topic": KAFKA_TOPIC
    }
