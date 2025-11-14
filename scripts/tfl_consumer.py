from kafka import KafkaConsumer
import json
import os
from datetime import datetime
from pathlib import Path

# Config
DATA_DIR = "/opt/airflow/data"
BRONZE_DIR = f"{DATA_DIR}/bronze"
KAFKA_TOPIC = "tfl_raw"
KAFKA_BROKER = "kafka:9092"

def consume():
    """
    Consume messages from Kafka and write to bronze layer as JSON files
    """
    # Ensure bronze directory exists
    Path(BRONZE_DIR).mkdir(parents=True, exist_ok=True)
    
    print(f"🎧 Starting Kafka consumer on topic: {KAFKA_TOPIC}")
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='tfl-bronze-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # Exit after 10 seconds of no messages
    )
    
    message_count = 0
    
    try:
        for message in consumer:
            data = message.value
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            
            # Write each message as a separate JSON file
            output_path = f"{BRONZE_DIR}/tfl_raw_{timestamp}_{message.offset}.json"
            
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            message_count += 1
            print(f"📦 Message {message_count} written to {output_path}")
            
    except Exception as e:
        print(f"❌ Consumer error: {e}")
    finally:
        consumer.close()
        print(f"✅ Consumed {message_count} messages to bronze layer")

if __name__ == "__main__":
    consume()