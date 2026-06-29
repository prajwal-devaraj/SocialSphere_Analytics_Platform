"""
Configuration for the ingestion service, loaded from environment variables
with sane local-dev defaults so `docker-compose up` works out of the box.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# --- MinIO / S3 ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "socialsphere")

# --- Bronze layer path prefix inside the bucket ---
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/events")

# --- API ---
API_TITLE = "SocialSphere Event Ingestion API"
API_VERSION = "1.0.0"

# --- Batching behavior ---
# Buffer events in memory and flush to a single object every N events or
# every FLUSH_INTERVAL_SECONDS, whichever comes first. Avoids writing one
# tiny file per event, which would be brutal at scale.
BUFFER_FLUSH_SIZE = int(os.environ.get("BUFFER_FLUSH_SIZE", "500"))
BUFFER_FLUSH_INTERVAL_SECONDS = int(os.environ.get("BUFFER_FLUSH_INTERVAL_SECONDS", "10"))
