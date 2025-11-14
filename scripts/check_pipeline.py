#!/usr/bin/env python3
"""
Diagnostic tool to check pipeline health and data flow
"""
import os
import json
from pathlib import Path
from datetime import datetime

def print_header(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def check_directories():
    """Check if required directories exist and show contents"""
    print_header("📁 DIRECTORY CHECK")
    
    dirs = {
        "Bronze Layer": "/opt/airflow/data/bronze",
        "Gold Layer": "/opt/airflow/data/gold",
        "Temp Directory": "/tmp/tfl_data",
        "Scripts": "/opt/airflow/scripts",
        "DAGs": "/opt/airflow/dags",
    }
    
    for name, path in dirs.items():
        p = Path(path)
        if p.exists():
            files = list(p.iterdir()) if p.is_dir() else []
            print(f"✅ {name:20s} EXISTS  ({len(files)} items)")
            
            # Show details for data directories
            if "data" in path or "tmp" in path:
                for f in files[:5]:  # Show first 5 items
                    if f.is_file():
                        size = f.stat().st_size
                        print(f"   └─ {f.name} ({size:,} bytes)")
        else:
            print(f"❌ {name:20s} MISSING")

def check_bronze_data():
    """Check bronze layer data quality"""
    print_header("🔍 BRONZE LAYER DATA CHECK")
    
    bronze_dir = Path("/opt/airflow/data/bronze")
    
    if not bronze_dir.exists():
        print("❌ Bronze directory does not exist!")
        return
    
    json_files = list(bronze_dir.glob("*.json"))
    
    if not json_files:
        print("⚠️  No JSON files found in bronze layer")
        print("💡 Run the ingestion task first: ingest_tfl_data -> consume_kafka_to_bronze")
        return
    
    print(f"📊 Found {len(json_files)} JSON file(s)")
    
    # Analyze first file
    for json_file in json_files[:3]:
        print(f"\n📄 Analyzing: {json_file.name}")
        
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                
            # Determine if it's a list or dict
            if isinstance(data, list):
                print(f"   Type: List with {len(data)} items")
                if data:
                    sample = data[0]
                    print(f"   Sample keys: {list(sample.keys())[:10]}")
            elif isinstance(data, dict):
                print(f"   Type: Dictionary with {len(data)} keys")
                print(f"   Keys: {list(data.keys())[:10]}")
            else:
                print(f"   Type: {type(data)}")
                
        except json.JSONDecodeError as e:
            print(f"   ❌ Invalid JSON: {e}")
        except Exception as e:
            print(f"   ❌ Error reading file: {e}")

def check_gold_data():
    """Check gold layer data"""
    print_header("💎 GOLD LAYER DATA CHECK")
    
    gold_dir = Path("/opt/airflow/data/gold")
    
    if not gold_dir.exists():
        print("⚠️  Gold directory does not exist")
        return
    
    parquet_files = list(gold_dir.glob("*.parquet"))
    
    if not parquet_files:
        print("⚠️  No parquet files found in gold layer")
        print("💡 Run the transform task: transform_with_spark")
        return
    
    print(f"✅ Found {len(parquet_files)} parquet file(s)")
    
    total_size = sum(f.stat().st_size for f in parquet_files)
    print(f"📊 Total size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")

def check_kafka_connectivity():
    """Test Kafka connection"""
    print_header("🔌 KAFKA CONNECTIVITY CHECK")
    
    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable
        
        consumer = KafkaConsumer(
            bootstrap_servers='kafka:9092',
            consumer_timeout_ms=5000
        )
        
        topics = consumer.topics()
        print(f"✅ Successfully connected to Kafka")
        print(f"📋 Available topics: {list(topics)}")
        
        consumer.close()
        
    except NoBrokersAvailable:
        print("❌ Cannot connect to Kafka broker")
        print("💡 Check if Kafka service is running: docker-compose ps")
    except ImportError:
        print("⚠️  kafka-python not installed")
    except Exception as e:
        print(f"❌ Kafka check failed: {e}")

def check_pyspark():
    """Test PySpark installation"""
    print_header("⚡ PYSPARK CHECK")
    
    try:
        from pyspark.sql import SparkSession
        
        # Try to create a minimal Spark session
        spark = SparkSession.builder \
            .appName("diagnostic_test") \
            .master("local[1]") \
            .config("spark.driver.memory", "512m") \
            .getOrCreate()
        
        # Create a simple DataFrame
        data = [("test", 1)]
        df = spark.createDataFrame(data, ["col1", "col2"])
        count = df.count()
        
        spark.stop()
        
        print(f"✅ PySpark is working correctly")
        print(f"   Test DataFrame created with {count} row(s)")
        
    except ImportError as e:
        print(f"❌ PySpark not installed: {e}")
    except Exception as e:
        print(f"❌ PySpark test failed: {e}")

def check_aws_credentials():
    """Check AWS configuration"""
    print_header("☁️  AWS CONFIGURATION CHECK")
    
    aws_key = os.environ.get('AWS_ACCESS_KEY_ID', None)
    aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
    aws_region = os.environ.get('AWS_DEFAULT_REGION', None)
    s3_bucket = os.environ.get('S3_BUCKET', None)
    
    if aws_key:
        print(f"✅ AWS_ACCESS_KEY_ID: {aws_key[:8]}...")
    else:
        print("❌ AWS_ACCESS_KEY_ID not set")
        
    if aws_secret:
        print(f"✅ AWS_SECRET_ACCESS_KEY: {'*' * 20}")
    else:
        print("❌ AWS_SECRET_ACCESS_KEY not set")
        
    if aws_region:
        print(f"✅ AWS_DEFAULT_REGION: {aws_region}")
    else:
        print("⚠️  AWS_DEFAULT_REGION not set (will use default)")
        
    if s3_bucket:
        print(f"✅ S3_BUCKET: {s3_bucket}")
    else:
        print("⚠️  S3_BUCKET not set")
    
    # Try to connect to S3
    if aws_key and aws_secret:
        try:
            import boto3
            s3 = boto3.client('s3')
            buckets = s3.list_buckets()
            print(f"\n✅ Successfully connected to AWS S3")
            print(f"📦 Available buckets: {len(buckets.get('Buckets', []))}")
        except Exception as e:
            print(f"\n❌ Failed to connect to AWS: {e}")

def main():
    """Run all diagnostic checks"""
    print("\n")
    print("🏥" * 35)
    print("  TfL PIPELINE HEALTH CHECK")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"))
    print("🏥" * 35)
    
    check_directories()
    check_bronze_data()
    check_gold_data()
    check_kafka_connectivity()
    check_pyspark()
    check_aws_credentials()
    
    print("\n" + "=" * 70)
    print("  DIAGNOSTIC CHECK COMPLETE")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()