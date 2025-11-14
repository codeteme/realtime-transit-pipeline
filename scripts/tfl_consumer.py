#!/usr/bin/env python3
"""
Kafka Consumer - reads messages and writes to bronze layer
"""
import sys
import json
import os
from datetime import datetime
from pathlib import Path

def consume():
    try:
        from kafka import KafkaConsumer
    except ImportError as e:
        print(f"❌ Import error: {e}")
        sys.exit(1)
    
    # Config
    DATA_DIR = "/opt/airflow/data"
    BRONZE_DIR = f"{DATA_DIR}/bronze"
    KAFKA_TOPIC = "tfl_raw"
    KAFKA_BROKER = "kafka:9092"
    
    # Ensure bronze directory exists
    print(f"📁 Ensuring bronze directory exists: {BRONZE_DIR}")
    Path(BRONZE_DIR).mkdir(parents=True, exist_ok=True)
    
    # Test write permissions
    test_file = os.path.join(BRONZE_DIR, ".test_write")
    try:
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        print(f"✅ Bronze directory is writable")
    except Exception as e:
        print(f"❌ Cannot write to bronze directory: {e}")
        sys.exit(1)
    
    print(f"🎧 Starting Kafka consumer on topic: {KAFKA_TOPIC}")
    
    # Create Kafka consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='tfl-bronze-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=20000,  # Exit after 20 seconds of no messages
            max_poll_records=100
        )
        print("✅ Consumer connected to Kafka")
    except Exception as e:
        print(f"❌ Failed to create consumer: {e}")
        sys.exit(1)
    
    message_count = 0
    error_count = 0
    
    try:
        print("📦 Consuming messages...")
        for message in consumer:
            try:
                data = message.value
                
                if data is None:
                    print("⚠️  Received null message, skipping")
                    continue
                
                # Generate unique filename
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
                filename = f"tfl_raw_{timestamp}_p{message.partition}_o{message.offset}.json"
                output_path = os.path.join(BRONZE_DIR, filename)
                
                # Write to file
                with open(output_path, 'w') as f:
                    json.dump(data, f, indent=2)
                
                message_count += 1
                
                # Log progress
                if message_count <= 5:
                    line = data.get('lineName', 'N/A')
                    station = data.get('stationName', 'N/A')
                    print(f"  #{message_count}: {line} at {station}")
                elif message_count % 50 == 0:
                    print(f"  Processed {message_count} messages...")
                    
            except Exception as e:
                error_count += 1
                print(f"⚠️  Error processing message: {e}")
                if error_count > 20:
                    print("❌ Too many errors, stopping")
                    break
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"❌ Consumer error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        
    print(f"\n✅ Consumed {message_count} messages to bronze layer")
    print(f"   Errors: {error_count}")
    
    # List files created
    files = sorted([f for f in os.listdir(BRONZE_DIR) if f.startswith('tfl_raw_')])
    print(f"📁 Total files in bronze: {len(files)}")
    
    if message_count == 0:
        print("⚠️  No messages consumed - topic might be empty")
        # Don't fail if topic is empty
        sys.exit(0)
    
    return message_count

if __name__ == "__main__":
    try:
        count = consume()
        print(f"\n🎉 Consumption complete: {count} messages")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)