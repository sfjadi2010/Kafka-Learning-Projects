import requests
import time

# API endpoints
PRODUCER_URL = "http://localhost:8000"
CONSUMER_URL = "http://localhost:8001"

def test_producer_health():
    """Test producer API health"""
    print("Testing Producer API health...")
    response = requests.get(f"{PRODUCER_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}\n")

def upload_csv():
    """Upload CSV file to Kafka via producer API"""
    print("Uploading CSV file...")
    with open('sample_data.csv', 'rb') as f:
        files = {'file': ('sample_data.csv', f, 'text/csv')}
        response = requests.post(f"{PRODUCER_URL}/upload-csv", files=files)
    
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}\n")
    return response.json()

def start_consumer():
    """Start the Kafka consumer"""
    print("Starting Kafka consumer...")
    response = requests.post(f"{CONSUMER_URL}/start-consumer")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}\n")

def get_consumer_stats():
    """Get consumer statistics"""
    print("Getting consumer statistics...")
    response = requests.get(f"{CONSUMER_URL}/stats")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}\n")

def get_records():
    """Get records from SQLite"""
    print("Getting records from database...")
    response = requests.get(f"{CONSUMER_URL}/records?limit=5")
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Total records: {data['total']}")
    if data['records']:
        print("Sample records:")
        for record in data['records'][:3]:
            print(f"  - Row {record['row_number']}: {record['data']}")
    print()

def get_record_count():
    """Get total record count"""
    print("Getting total record count...")
    response = requests.get(f"{CONSUMER_URL}/records/count")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}\n")

if __name__ == "__main__":
    print("=" * 60)
    print("Testing Kafka Producer & Consumer APIs")
    print("=" * 60 + "\n")
    
    try:
        # Test producer
        test_producer_health()
        
        # Upload CSV
        upload_result = upload_csv()
        
        # Start consumer
        start_consumer()
        
        # Wait for consumer to process messages
        print("Waiting 5 seconds for consumer to process messages...")
        time.sleep(5)
        
        # Get stats
        get_consumer_stats()
        
        # Get record count
        get_record_count()
        
        # Get records
        get_records()
        
        print("=" * 60)
        print("Testing completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error: {e}")
