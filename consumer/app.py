from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
import sqlite3
import json
import logging
import threading
import re
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

def get_table_name_from_topic(topic: str) -> str:
    """Generate a valid SQLite table name from topic name"""
    # Replace special characters with underscores and prefix with 'topic_'
    table_name = f"topic_{topic.replace('-', '_').replace('.', '_')}"
    return table_name

def create_topic_table(topic: str):
    """Create a table for a specific topic if it doesn't exist"""
    table_name = get_table_name_from_topic(topic)
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                row_number INTEGER,
                filename TEXT,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        logger.info(f"Created/verified table: {table_name}")

def init_database():
    """Initialize SQLite database and create base tables"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Create a topics metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS topics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_name TEXT UNIQUE,
                table_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        logger.info("Database initialized successfully")

def consume_kafka_messages():
    """Background task to consume Kafka messages from all topics and store in SQLite"""
    global is_consuming
    
    try:
        # Subscribe to all topics using pattern (except internal Kafka topics)
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            api_version=(2, 0, 2)
        )
        
        # Subscribe to all topics except internal ones
        consumer.subscribe(pattern='^(?!__).*')
        
        logger.info(f"Kafka consumer started, subscribing to all topics")
        is_consuming = True
        
        for message in consumer:
            if not is_consuming:
                break
                
            try:
                data = message.value
                topic = message.topic
                
                # Create table for this topic if it doesn't exist
                create_topic_table(topic)
                table_name = get_table_name_from_topic(topic)
                
                # Store in topic-specific table
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Register topic if not already registered
                    cursor.execute('''
                        INSERT OR IGNORE INTO topics (topic_name, table_name)
                        VALUES (?, ?)
                    ''', (topic, table_name))
                    
                    # Insert data into topic-specific table
                    cursor.execute(f'''
                        INSERT INTO {table_name} (row_number, filename, data)
                        VALUES (?, ?, ?)
                    ''', (
                        data.get('row_number'),
                        data.get('filename'),
                        json.dumps(data.get('data'))
                    ))
                    
                    conn.commit()
                    logger.info(f"Stored record from topic '{topic}', file {data.get('filename')}, row {data.get('row_number')}")
                    
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
            "/topics": "GET - Get all topics",
            "/topics/{topic}/records": "GET - Get records for specific topic",
            "/records": "GET - Get all stored records (deprecated)",
            "/records/count": "GET - Get record count (deprecated)",
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
        
        # Get total count
        cursor.execute('SELECT COUNT(*) as total FROM csv_records')
        total_count = cursor.fetchone()['total']
        
        # Get paginated records
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
        "total": total_count,
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
        
        # Get actual record count from csv_records table
        cursor.execute('SELECT COUNT(*) as total FROM csv_records')
        total_count = cursor.fetchone()['total']
        
        # Get last record timestamp
        cursor.execute('SELECT created_at FROM csv_records ORDER BY created_at DESC LIMIT 1')
        last_record = cursor.fetchone()
        last_consumed_at = last_record['created_at'] if last_record else None
        
        # Determine status based on consumer state and record count
        if is_consuming:
            status = "active"
        elif total_count > 0:
            status = "idle"
        else:
            status = "not_started"
        
        stats = {
            "total_messages_consumed": total_count,
            "last_consumed_at": last_consumed_at,
            "status": status,
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

@app.get("/topics")
async def get_topics():
    """Get all registered topics with their metadata"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get all topics from metadata table
        cursor.execute('''
            SELECT topic_name, table_name, created_at
            FROM topics
            ORDER BY created_at DESC
        ''')
        
        topics_data = cursor.fetchall()
        
        # Get record count for each topic
        topics = []
        for topic in topics_data:
            cursor.execute(f'SELECT COUNT(*) as count FROM {topic["table_name"]}')
            count = cursor.fetchone()['count']
            
            topics.append({
                "topic_name": topic["topic_name"],
                "table_name": topic["table_name"],
                "record_count": count,
                "created_at": topic["created_at"]
            })
    
    return {"topics": topics}

@app.get("/topics/{topic}/records")
async def get_topic_records(topic: str, limit: int = 100, offset: int = 0):
    """Get records for a specific topic"""
    table_name = get_table_name_from_topic(topic)
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Verify topic exists
        cursor.execute('SELECT topic_name FROM topics WHERE topic_name = ?', (topic,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found")
        
        # Get total count
        cursor.execute(f'SELECT COUNT(*) as total FROM {table_name}')
        total_count = cursor.fetchone()['total']
        
        # Get paginated records
        cursor.execute(f'''
            SELECT id, row_number, filename, data, created_at
            FROM {table_name}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        
        rows = cursor.fetchall()
        
        records = []
        for row in rows:
            record = {
                "id": row["id"],
                "row_number": row["row_number"],
                "filename": row["filename"],
                "data": json.loads(row["data"]) if row["data"] else {},
                "created_at": row["created_at"]
            }
            records.append(record)
    
    return {
        "topic": topic,
        "records": records,
        "total": total_count,
        "limit": limit,
        "offset": offset
    }
