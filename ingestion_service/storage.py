"""
Storage layer for the ingestion service. Buffers validated events in memory
and flushes them to MinIO (S3-compatible) as newline-delimited JSON objects,
partitioned by event date — matching the layout used by the event generator
and expected by the bronze_to_silver Spark job.

Object key layout:
    bronze/events/year=YYYY/month=MM/day=DD/hour=HH/part-<uuid>.json
"""

import json
import threading
import time
import uuid
from datetime import datetime
from collections import defaultdict
from typing import List, Dict

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    BRONZE_PREFIX,
    BUFFER_FLUSH_SIZE,
    BUFFER_FLUSH_INTERVAL_SECONDS,
)


class EventStore:
    """
    Thread-safe in-memory buffer that periodically flushes batches of
    events to MinIO/S3, grouped by their event date partition.
    """

    def __init__(self):
        self._client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=BotoConfig(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._buffer: List[dict] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._total_ingested = 0
        self._total_flushed = 0
        self._ensure_bucket()

        # Background flush thread so events don't sit in memory too long
        # even if traffic is low.
        self._stop_flag = False
        self._flush_thread = threading.Thread(target=self._background_flush_loop, daemon=True)
        self._flush_thread.start()

    def _ensure_bucket(self):
        try:
            self._client.head_bucket(Bucket=MINIO_BUCKET)
        except ClientError:
            try:
                self._client.create_bucket(Bucket=MINIO_BUCKET)
            except ClientError as e:
                print(f"[EventStore] warning: could not create/verify bucket: {e}")

    def add_event(self, event: dict):
        with self._lock:
            self._buffer.append(event)
            self._total_ingested += 1
            should_flush = len(self._buffer) >= BUFFER_FLUSH_SIZE
        if should_flush:
            self.flush()

    def add_events(self, events: List[dict]):
        with self._lock:
            self._buffer.extend(events)
            self._total_ingested += len(events)
            should_flush = len(self._buffer) >= BUFFER_FLUSH_SIZE
        if should_flush:
            self.flush()

    def _background_flush_loop(self):
        while not self._stop_flag:
            time.sleep(1)
            if time.time() - self._last_flush >= BUFFER_FLUSH_INTERVAL_SECONDS:
                self.flush()

    def flush(self):
        with self._lock:
            if not self._buffer:
                self._last_flush = time.time()
                return
            batch = self._buffer
            self._buffer = []
            self._last_flush = time.time()

        grouped: Dict[str, list] = defaultdict(list)
        for event in batch:
            ts = event.get("event_timestamp")
            if isinstance(ts, str):
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except ValueError:
                    dt = datetime.utcnow()
            elif isinstance(ts, datetime):
                dt = ts
            else:
                dt = datetime.utcnow()

            partition = (
                f"year={dt.year:04d}/month={dt.month:02d}/"
                f"day={dt.day:02d}/hour={dt.hour:02d}"
            )
            grouped[partition].append(event)

        for partition, events in grouped.items():
            key = f"{BRONZE_PREFIX}/{partition}/part-{uuid.uuid4().hex[:12]}.json"
            body = "\n".join(json.dumps(e, default=str) for e in events)
            try:
                self._client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=body.encode("utf-8"))
                self._total_flushed += len(events)
            except ClientError as e:
                print(f"[EventStore] error writing to {key}: {e}")
                # Put events back so we don't lose them silently
                with self._lock:
                    self._buffer.extend(events)

    def stats(self) -> dict:
        with self._lock:
            buffered = len(self._buffer)
        return {
            "total_ingested": self._total_ingested,
            "total_flushed": self._total_flushed,
            "currently_buffered": buffered,
            "bucket": MINIO_BUCKET,
            "prefix": BRONZE_PREFIX,
        }


# Singleton instance used by the FastAPI app
event_store = EventStore()
