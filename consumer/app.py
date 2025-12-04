from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
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
        
        # Count total records across all topic tables
        total_count = 0
        cursor.execute('SELECT table_name FROM topics')
        topics = cursor.fetchall()
        
        for topic in topics:
            table_name = topic['table_name']
            cursor.execute(f'SELECT COUNT(*) as count FROM {table_name}')
            total_count += cursor.fetchone()['count']
    
    return {
        "status": "healthy",
        "database": "connected",
        "consumer_active": is_consuming,
        "total_records": total_count
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
        
        # Count records across all topic tables
        total_count = 0
        cursor.execute('SELECT table_name FROM topics')
        topics = cursor.fetchall()
        
        for topic in topics:
            table_name = topic['table_name']
            cursor.execute(f'SELECT COUNT(*) as count FROM {table_name}')
            total_count += cursor.fetchone()['count']
    
    return {"total_records": total_count}

@app.get("/stats")
async def get_stats():
    """Get consumer statistics"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get total record count across all topic tables
        total_count = 0
        last_consumed_at = None
        
        cursor.execute('SELECT table_name FROM topics')
        topics = cursor.fetchall()
        
        for topic in topics:
            table_name = topic['table_name']
            cursor.execute(f'SELECT COUNT(*) as count FROM {table_name}')
            total_count += cursor.fetchone()['count']
            
            # Get latest timestamp from this table
            cursor.execute(f'SELECT created_at FROM {table_name} ORDER BY created_at DESC LIMIT 1')
            last_record = cursor.fetchone()
            if last_record:
                record_time = last_record['created_at']
                if last_consumed_at is None or record_time > last_consumed_at:
                    last_consumed_at = record_time
        
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
    """Delete all records from all topic tables and Kafka topics after archiving to audit tables"""
    global is_consuming
    
    # Stop consumer first to prevent recreation of topics
    was_running = is_consuming
    if is_consuming:
        is_consuming = False
        logger.info("Stopped consumer for deletion")
        # Give it a moment to stop
        import time
        time.sleep(2)
    
    topics_to_delete = []
    audited_tables = []
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get all topic names and tables
        cursor.execute('SELECT topic_name, table_name FROM topics')
        topics = cursor.fetchall()
        
        # Collect topic names for Kafka deletion
        topics_to_delete = [topic['topic_name'] for topic in topics]
        
        # Archive and delete records from each topic table
        for topic in topics:
            table_name = topic['table_name']
            audit_table_name = f"audit_{table_name}"
            
            try:
                # Create audit table as a copy of the original table structure
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {audit_table_name} AS 
                    SELECT *, datetime('now') as archived_at 
                    FROM {table_name} 
                    WHERE 1=0
                ''')
                
                # If table is empty, create it with the schema
                cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                count = cursor.fetchone()['count']
                
                if count == 0:
                    # Create audit table with same structure
                    cursor.execute(f'''
                        CREATE TABLE IF NOT EXISTS {audit_table_name} (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            row_number INTEGER,
                            filename TEXT,
                            data TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                else:
                    # Copy all data to audit table with archive timestamp
                    cursor.execute(f'''
                        INSERT INTO {audit_table_name} 
                        SELECT *, datetime('now') as archived_at 
                        FROM {table_name}
                    ''')
                
                # Drop the original table
                cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
                audited_tables.append(audit_table_name)
                logger.info(f"Archived {count} records from {table_name} to {audit_table_name}")
                
            except Exception as e:
                logger.error(f"Error archiving table {table_name}: {str(e)}")
        
        # Archive topics metadata
        try:
            # Create audit_topics table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS audit_topics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic_name TEXT,
                    table_name TEXT,
                    created_at TIMESTAMP,
                    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Copy topics to audit
            cursor.execute('''
                INSERT INTO audit_topics (topic_name, table_name, created_at)
                SELECT topic_name, table_name, created_at FROM topics
            ''')
            
            # Delete all topics metadata
            cursor.execute('DELETE FROM topics')
            logger.info("Archived topics metadata to audit_topics")
        except Exception as e:
            logger.error(f"Error archiving topics metadata: {str(e)}")
        
        # Handle old tables if they exist (for backwards compatibility)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='csv_records'")
        if cursor.fetchone():
            try:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS audit_csv_records AS 
                    SELECT *, datetime('now') as archived_at 
                    FROM csv_records
                ''')
                cursor.execute("DROP TABLE IF EXISTS csv_records")
                audited_tables.append("audit_csv_records")
            except Exception as e:
                logger.error(f"Error archiving csv_records: {str(e)}")
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='consumer_stats'")
        if cursor.fetchone():
            try:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS audit_consumer_stats AS 
                    SELECT *, datetime('now') as archived_at 
                    FROM consumer_stats
                ''')
                cursor.execute("DROP TABLE IF EXISTS consumer_stats")
                audited_tables.append("audit_consumer_stats")
            except Exception as e:
                logger.error(f"Error archiving consumer_stats: {str(e)}")
        
        conn.commit()
    
    # Delete Kafka topics
    deleted_topics = []
    failed_topics = []
    
    if topics_to_delete:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id='consumer-admin'
            )
            
            # Delete topics from Kafka
            result = admin_client.delete_topics(topics_to_delete, timeout_ms=5000)
            
            # Wait for deletion to complete
            for topic, future in result.items():
                try:
                    future.result()  # Block until topic is deleted
                    deleted_topics.append(topic)
                    logger.info(f"Successfully deleted Kafka topic: {topic}")
                except Exception as e:
                    failed_topics.append(topic)
                    logger.error(f"Failed to delete Kafka topic {topic}: {str(e)}")
            
            admin_client.close()
        except Exception as e:
            logger.error(f"Error connecting to Kafka admin: {str(e)}")
            return {
                "status": "partial_success",
                "message": "Data archived but failed to connect to Kafka",
                "audited_tables": audited_tables,
                "error": str(e)
            }
    
    message = "All records archived and deleted, Kafka topics removed"
    if failed_topics:
        message += f" (Failed to delete Kafka topics: {', '.join(failed_topics)})"
    
    return {
        "status": "success",
        "message": message,
        "audited_tables": audited_tables,
        "deleted_kafka_topics": deleted_topics,
        "failed_kafka_topics": failed_topics
    }

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

@app.delete("/topics/{topic}")
async def delete_topic(topic: str):
    """Delete a specific topic's data and Kafka topic after archiving"""
    table_name = get_table_name_from_topic(topic)
    audit_table_name = f"audit_{table_name}"
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Verify topic exists
        cursor.execute('SELECT topic_name, table_name FROM topics WHERE topic_name = ?', (topic,))
        topic_row = cursor.fetchone()
        if not topic_row:
            raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found")
        
        try:
            # Get record count
            cursor.execute(f'SELECT COUNT(*) as count FROM {table_name}')
            count = cursor.fetchone()['count']
            
            # Archive the data
            if count > 0:
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {audit_table_name} AS 
                    SELECT *, datetime('now') as archived_at 
                    FROM {table_name} 
                    WHERE 1=0
                ''')
                
                cursor.execute(f'''
                    INSERT INTO {audit_table_name} 
                    SELECT *, datetime('now') as archived_at 
                    FROM {table_name}
                ''')
                logger.info(f"Archived {count} records from {table_name} to {audit_table_name}")
            else:
                # Create empty audit table
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {audit_table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        row_number INTEGER,
                        filename TEXT,
                        data TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            
            # Drop the original table
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            
            # Remove from topics metadata
            cursor.execute('DELETE FROM topics WHERE topic_name = ?', (topic,))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error archiving topic {topic}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to archive topic: {str(e)}")
    
    # Delete Kafka topic
    kafka_deleted = False
    kafka_error = None
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='consumer-admin'
        )
        
        result = admin_client.delete_topics([topic], timeout_ms=5000)
        
        for topic_name, future in result.items():
            try:
                future.result()
                kafka_deleted = True
                logger.info(f"Successfully deleted Kafka topic: {topic_name}")
            except Exception as e:
                kafka_error = str(e)
                logger.error(f"Failed to delete Kafka topic {topic_name}: {str(e)}")
        
        admin_client.close()
    except Exception as e:
        kafka_error = str(e)
        logger.error(f"Error connecting to Kafka admin: {str(e)}")
    
    return {
        "status": "success",
        "message": f"Topic '{topic}' archived and deleted",
        "archived_table": audit_table_name,
        "records_archived": count,
        "kafka_topic_deleted": kafka_deleted,
        "kafka_error": kafka_error
    }

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
