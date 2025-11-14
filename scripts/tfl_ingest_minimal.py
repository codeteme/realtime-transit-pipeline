#!/usr/bin/env python3
"""
TfL Data Ingestion - sends arrivals to Kafka
"""
import sys
import json
import time

def ingest():
    try:
        from kafka import KafkaProducer
        import requests
        from datetime import datetime
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Install with: pip install kafka-python requests")
        sys.exit(1)
    
    # Configuration
    TFL_URL = "https://api.tfl.gov.uk/Line/central/Arrivals"
    KAFKA_BROKER = "kafka:9092"
    KAFKA_TOPIC = "tfl_raw"
    
    print(f"📡 Fetching from TfL API: {TFL_URL}")
    
    # Fetch data from TfL
    try:
        response = requests.get(TFL_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not isinstance(data, list):
            print(f"⚠️  Unexpected response format: {type(data)}")
            print(f"Response: {data}")
            sys.exit(1)
            
        print(f"✅ Received {len(data)} arrival records")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        sys.exit(1)
    
    # Connect to Kafka
    print(f"🔌 Connecting to Kafka at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            max_block_ms=10000,  # Wait up to 10 seconds for connection
            request_timeout_ms=30000
        )
        print("✅ Connected to Kafka")
        
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        print(f"Is Kafka running? Check: docker-compose ps kafka")
        sys.exit(1)
    
    # Send messages
    print(f"📨 Sending {len(data)} messages to topic '{KAFKA_TOPIC}'...")
    sent_count = 0
    failed_count = 0
    
    for i, arrival in enumerate(data):
        try:
            # Add metadata
            arrival['ingested_at'] = datetime.utcnow().isoformat()
            arrival['batch_index'] = i
            
            # Send to Kafka
            future = producer.send(KAFKA_TOPIC, arrival)
            future.get(timeout=10)  # Wait for confirmation
            sent_count += 1
            
            if sent_count % 50 == 0:
                print(f"  Sent {sent_count}/{len(data)} messages...")
                
        except Exception as e:
            failed_count += 1
            print(f"⚠️  Failed to send message {i}: {e}")
            if failed_count > 10:
                print("❌ Too many failures, aborting")
                sys.exit(1)
    
    # Flush and close
    producer.flush()
    producer.close()
    
    print(f"\n✅ Successfully sent {sent_count} messages to Kafka")
    print(f"   Failed: {failed_count}")
    
    if sent_count == 0:
        print("❌ No messages sent!")
        sys.exit(1)
    
    return sent_count

if __name__ == "__main__":
    try:
        count = ingest()
        print(f"\n🎉 Ingestion complete: {count} messages")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)