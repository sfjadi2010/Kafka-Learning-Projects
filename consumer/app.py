from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
import sqlite3
import json
import logging
import threading
from contextlib import contextmanager
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Kafka Consumer & SQLite Storage API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9093'
KAFKA_TOPIC = 'csv-data'
KAFKA_GROUP_ID = 'csv-consumer-group'

# SQLite configuration
DB_PATH = '/app/data/kafka_data.db'

# Global consumer thread
consumer_thread = None
is_consuming = False

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_database():
    """Initialize SQLite database and create tables"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS csv_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                row_number INTEGER,
                filename TEXT,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS consumer_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_messages INTEGER DEFAULT 0,
                last_consumed_at TIMESTAMP,
                status TEXT
            )
        ''')
        conn.commit()
        logger.info("Database initialized successfully")

def consume_kafka_messages():
    """Background task to consume Kafka messages and store in SQLite"""
    global is_consuming
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            api_version=(2, 0, 2)
        )
        
        logger.info(f"Kafka consumer started for topic: {KAFKA_TOPIC}")
        is_consuming = True
        
        for message in consumer:
            if not is_consuming:
                break
                
            try:
                data = message.value
                
                # Store in SQLite
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO csv_records (row_number, filename, data)
                        VALUES (?, ?, ?)
                    ''', (
                        data.get('row_number'),
                        data.get('filename'),
                        json.dumps(data.get('data'))
                    ))
                    
                    # Update stats
                    cursor.execute('''
                        UPDATE consumer_stats 
                        SET total_messages = total_messages + 1,
                            last_consumed_at = CURRENT_TIMESTAMP,
                            status = 'active'
                        WHERE id = 1
                    ''')
                    
                    if cursor.rowcount == 0:
                        cursor.execute('''
                            INSERT INTO consumer_stats (total_messages, last_consumed_at, status)
                            VALUES (1, CURRENT_TIMESTAMP, 'active')
                        ''')
                    
                    conn.commit()
                    logger.info(f"Stored record from {data.get('filename')}, row {data.get('row_number')}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
        is_consuming = False
    finally:
        consumer.close()
        logger.info("Kafka consumer stopped")

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    init_database()
    logger.info("Application started")

@app.get("/")
async def root():
    return {
        "message": "Kafka Consumer & SQLite Storage API",
        "endpoints": {
            "/start-consumer": "POST - Start Kafka consumer",
            "/stop-consumer": "POST - Stop Kafka consumer",
            "/records": "GET - Get all stored records",
            "/records/count": "GET - Get record count",
            "/stats": "GET - Get consumer statistics",
            "/health": "GET - Health check"
        }
    }

@app.get("/health")
async def health_check():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM csv_records")
        count = cursor.fetchone()[0]
    
    return {
        "status": "healthy",
        "database": "connected",
        "consumer_active": is_consuming,
        "total_records": count
    }

@app.post("/start-consumer")
async def start_consumer():
    """Start the Kafka consumer in a background thread"""
    global consumer_thread, is_consuming
    
    if is_consuming:
        return {"status": "already_running", "message": "Consumer is already running"}
    
    consumer_thread = threading.Thread(target=consume_kafka_messages, daemon=True)
    consumer_thread.start()
    
    return {
        "status": "started",
        "message": "Kafka consumer started successfully",
        "topic": KAFKA_TOPIC
    }

@app.post("/stop-consumer")
async def stop_consumer():
    """Stop the Kafka consumer"""
    global is_consuming
    
    if not is_consuming:
        return {"status": "not_running", "message": "Consumer is not running"}
    
    is_consuming = False
    
    # Update status in database
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE consumer_stats 
            SET status = 'stopped'
            WHERE id = 1
        ''')
        conn.commit()
    
    return {
        "status": "stopped",
        "message": "Kafka consumer stopped successfully"
    }

@app.get("/records")
async def get_records(limit: int = 100, offset: int = 0):
    """Get stored records from SQLite"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, row_number, filename, data, created_at
            FROM csv_records
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        
        rows = cursor.fetchall()
        
        records = []
        for row in rows:
            records.append({
                "id": row['id'],
                "row_number": row['row_number'],
                "filename": row['filename'],
                "data": json.loads(row['data']),
                "created_at": row['created_at']
            })
    
    return {
        "total": len(records),
        "limit": limit,
        "offset": offset,
        "records": records
    }

@app.get("/records/count")
async def get_record_count():
    """Get total count of stored records"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM csv_records")
        count = cursor.fetchone()[0]
    
    return {"total_records": count}

@app.get("/stats")
async def get_stats():
    """Get consumer statistics"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT total_messages, last_consumed_at, status
            FROM consumer_stats
            WHERE id = 1
        ''')
        
        row = cursor.fetchone()
        
        if row:
            stats = {
                "total_messages_consumed": row['total_messages'],
                "last_consumed_at": row['last_consumed_at'],
                "status": row['status'],
                "current_consumer_active": is_consuming
            }
        else:
            stats = {
                "total_messages_consumed": 0,
                "last_consumed_at": None,
                "status": "not_started",
                "current_consumer_active": is_consuming
            }
    
    return stats

@app.delete("/records")
async def delete_all_records():
    """Delete all records from database"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM csv_records")
        cursor.execute("DELETE FROM consumer_stats")
        conn.commit()
    
    return {"status": "success", "message": "All records deleted"}
