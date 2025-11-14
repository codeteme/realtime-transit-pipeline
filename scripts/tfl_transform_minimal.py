from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pathlib import Path
import os
import sys
import traceback
import glob
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
DATA_DIR = "/opt/airflow/data"
BRONZE_DIR = f"{DATA_DIR}/bronze"
GOLD_DIR = f"{DATA_DIR}/gold"

# -----------------------------
# DIAGNOSTIC FUNCTION
# -----------------------------
def check_environment():
    """Check and print environment info"""
    print("=" * 60)
    print("🔍 ENVIRONMENT CHECK")
    print("=" * 60)
    
    # Check Java
    java_home = os.environ.get('JAVA_HOME', 'NOT SET')
    print(f"JAVA_HOME: {java_home}")
    
    # Check directories
    print(f"\n📁 DATA_DIR: {DATA_DIR}")
    print(f"📁 BRONZE_DIR: {BRONZE_DIR}")
    print(f"📁 GOLD_DIR: {GOLD_DIR}")
    
    # Check bronze files
    bronze_path = Path(BRONZE_DIR)
    if bronze_path.exists():
        bronze_files = list(bronze_path.glob("*.json"))
        print(f"✅ Bronze directory exists")
        print(f"📊 Found {len(bronze_files)} JSON files in bronze")
        
        if bronze_files:
            print(f"📄 Sample files:")
            for f in bronze_files[:3]:
                size = f.stat().st_size
                print(f"   - {f.name} ({size} bytes)")
        else:
            print("⚠️  WARNING: No JSON files found in bronze directory!")
    else:
        print(f"❌ Bronze directory does not exist!")
        
    # Check gold directory
    gold_path = Path(GOLD_DIR)
    if gold_path.exists():
        print(f"✅ Gold directory exists")
    else:
        print(f"⚠️  Gold directory does not exist, will create it")
        gold_path.mkdir(parents=True, exist_ok=True)
        
    # Check temp directory
    tmp_dir = Path("/tmp/tfl_data")
    if not tmp_dir.exists():
        print(f"⚠️  /tmp/tfl_data does not exist, creating it")
        tmp_dir.mkdir(parents=True, exist_ok=True)
    else:
        print(f"✅ /tmp/tfl_data exists")
        
    print("=" * 60)

# -----------------------------
# MAIN FUNCTION
# -----------------------------
def main():
    spark = None
    try:
        # Run diagnostics
        check_environment()
        
        # Generate timestamp for this run
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        JSON_OUTPUT_DIR = f"/tmp/tfl_data/transformed_{timestamp}"
        JSON_FINAL = f"/tmp/tfl_data/transformed_{timestamp}.json"
        
        print(f"\n🚀 Starting transformation at {timestamp}")
        print("=" * 60)
        
        # Check if bronze has data
        bronze_files = list(Path(BRONZE_DIR).glob("*.json"))
        if not bronze_files:
            print("❌ ERROR: No JSON files found in bronze directory")
            print("💡 TIP: Check if the 'consume_kafka_to_bronze' task ran successfully")
            print("💡 TIP: Verify data in bronze directory:")
            print(f"   docker-compose exec airflow-scheduler ls -lh {BRONZE_DIR}")
            sys.exit(1)
            
        print(f"✅ Found {len(bronze_files)} files in bronze layer")
        
        # -----------------------------
        # SPARK SESSION
        # -----------------------------
        print("\n🔧 Creating Spark session...")
        spark = (
            SparkSession.builder
            .appName("tfl_transform_minimal")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp")
            .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/tmp")
            .getOrCreate()
        )
        print("✅ Spark session created successfully")
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        # -----------------------------
        # READ RAW JSON
        # -----------------------------
        print(f"\n📖 Reading data from {BRONZE_DIR}")
        
        try:
            # Read with multiLine option to handle nested JSON
            df = spark.read.option("multiLine", "true").json(str(Path(BRONZE_DIR) / "*.json"))
            
            row_count = df.count()
            print(f"✅ Successfully loaded {row_count} rows")
            
            if row_count == 0:
                print("⚠️  Warning: DataFrame is empty (0 rows)")
                print("💡 Check bronze JSON file contents")
                sys.exit(1)
                
            # Show schema
            print("\n📋 DataFrame Schema:")
            df.printSchema()
            
            # Show sample data
            print("\n📊 Sample Data (first 3 rows):")
            df.show(3, truncate=False)
            
        except Exception as e:
            print(f"❌ ERROR reading JSON files: {e}")
            print("\n💡 Troubleshooting tips:")
            print("1. Check if JSON files are valid")
            print("2. Verify JSON structure matches expected schema")
            print("3. Try reading a single file manually")
            raise
        
        # -----------------------------
        # SELECT & CLEAN
        # -----------------------------
        print("\n🧹 Cleaning and transforming data...")
        
        try:
            # Check if required columns exist
            required_cols = ["lineName", "stationName", "expectedArrival", "timeToStation"]
            available_cols = df.columns
            missing_cols = [c for c in required_cols if c not in available_cols]
            
            if missing_cols:
                print(f"⚠️  WARNING: Missing columns: {missing_cols}")
                print(f"Available columns: {available_cols}")
                print("\n💡 Adjusting transformation to use available columns...")
            
            # Transform with available columns
            clean_df = df
            
            if "lineName" in available_cols:
                clean_df = clean_df.withColumn("route", col("lineName"))
            if "stationName" in available_cols:
                clean_df = clean_df.withColumn("stop_name", col("stationName"))
            if "expectedArrival" in available_cols:
                clean_df = clean_df.withColumn("expected_ts", col("expectedArrival"))
                clean_df = clean_df.withColumn("expected_time", to_timestamp(col("expected_ts")))
            if "timeToStation" in available_cols:
                clean_df = clean_df.withColumn("seconds_to_arrival", col("timeToStation"))
            
            # Add processing timestamp
            clean_df = clean_df.withColumn("processed_at", current_timestamp())
            
            # Select final columns (only those that exist)
            select_cols = []
            for c in ["route", "stop_name", "expected_ts", "expected_time", "seconds_to_arrival", "processed_at"]:
                if c in clean_df.columns:
                    select_cols.append(c)
                    
            clean_df = clean_df.select(*select_cols)
            
            clean_row_count = clean_df.count()
            print(f"✅ Cleaned data: {clean_row_count} rows")
            
            # Show cleaned sample
            print("\n📊 Cleaned Data Sample:")
            clean_df.show(5, truncate=False)
            
        except Exception as e:
            print(f"❌ ERROR during transformation: {e}")
            print("\n💡 Check column names and data types")
            raise
        
        # -----------------------------
        # WRITE TO PARQUET (for data lake)
        # -----------------------------
        print(f"\n💾 Writing Parquet to {GOLD_DIR}...")
        
        try:
            (
                clean_df.write
                .mode("overwrite")
                .parquet(GOLD_DIR)
            )
            print(f"✅ Parquet written to {GOLD_DIR}")
            
            # Verify parquet files
            parquet_files = list(Path(GOLD_DIR).glob("*.parquet"))
            print(f"📊 Created {len(parquet_files)} parquet files")
            
        except Exception as e:
            print(f"❌ ERROR writing Parquet: {e}")
            raise
        
        # -----------------------------
        # WRITE TO JSON (for S3 upload)
        # -----------------------------
        print(f"\n📝 Writing JSON to {JSON_OUTPUT_DIR}...")
        
        try:
            # Create directory if it doesn't exist
            os.makedirs("/tmp/tfl_data", exist_ok=True)
            
            # Write to temporary directory (Spark creates part files)
            (
                clean_df.coalesce(1)
                .write
                .mode("overwrite")
                .json(JSON_OUTPUT_DIR)
            )
            
            # Find the part file and rename it
            json_files = glob.glob(f"{JSON_OUTPUT_DIR}/part-*.json")
            
            if json_files:
                # Move the part file to final location
                os.rename(json_files[0], JSON_FINAL)
                
                # Clean up the temporary directory
                import shutil
                shutil.rmtree(JSON_OUTPUT_DIR, ignore_errors=True)
                
                # Verify file size
                file_size = Path(JSON_FINAL).stat().st_size
                print(f"✅ JSON written to {JSON_FINAL} ({file_size} bytes)")
            else:
                print(f"⚠️  No JSON part files found in {JSON_OUTPUT_DIR}")
                # List what's in the directory
                all_files = os.listdir(JSON_OUTPUT_DIR) if os.path.exists(JSON_OUTPUT_DIR) else []
                print(f"Directory contents: {all_files}")
                
        except Exception as e:
            print(f"❌ ERROR writing JSON: {e}")
            raise
        
        print("\n" + "=" * 60)
        print("🎉 Transformation completed successfully!")
        print("=" * 60)
        
        # Summary
        print("\n📊 TRANSFORMATION SUMMARY:")
        print(f"   Input files:  {len(bronze_files)}")
        print(f"   Rows processed: {clean_row_count}")
        print(f"   Gold parquet:   {GOLD_DIR}")
        print(f"   JSON export:    {JSON_FINAL}")
        print("=" * 60)
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ TRANSFORMATION FAILED")
        print("=" * 60)
        print(f"\nError: {str(e)}")
        print("\n📋 Full Traceback:")
        traceback.print_exc()
        print("\n" + "=" * 60)
        sys.exit(1)
        
    finally:
        if spark:
            try:
                spark.stop()
                print("\n🛑 Spark session stopped")
            except Exception:
                pass

# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == '__main__':
    main()