import boto3
import os
import glob
from datetime import datetime
from pathlib import Path

# Config
S3_BUCKET = os.getenv("S3_BUCKET", "tfl-transit-data")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
JSON_DIR = "/tmp/tfl_data"

def upload_to_s3():
    """
    Upload transformed JSON files to S3
    """
    print(f"☁️ Starting S3 upload to bucket: {S3_BUCKET}")
    
    # Find all JSON files
    json_files = glob.glob(f"{JSON_DIR}/transformed_*.json")
    
    if not json_files:
        print("⚠️ No JSON files found to upload")
        return
    
    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    
    # Create bucket if it doesn't exist
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
    except:
        print(f"📦 Creating bucket: {S3_BUCKET}")
        try:
            if AWS_REGION == 'us-east-1':
                s3_client.create_bucket(Bucket=S3_BUCKET)
            else:
                s3_client.create_bucket(
                    Bucket=S3_BUCKET,
                    CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                )
        except Exception as e:
            print(f"❌ Failed to create bucket: {e}")
            return
    
    # Upload each file
    upload_count = 0
    for local_file in json_files:
        filename = Path(local_file).name
        s3_key = f"gold/{datetime.utcnow().strftime('%Y/%m/%d')}/{filename}"
        
        try:
            s3_client.upload_file(local_file, S3_BUCKET, s3_key)
            print(f"✅ Uploaded: s3://{S3_BUCKET}/{s3_key}")
            upload_count += 1
            
            # Clean up local file after successful upload
            os.remove(local_file)
            
        except Exception as e:
            print(f"❌ Failed to upload {filename}: {e}")
    
    print(f"🎉 Upload complete: {upload_count} files uploaded to S3")

if __name__ == "__main__":
    upload_to_s3()