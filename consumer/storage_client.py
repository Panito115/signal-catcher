"""MinIO storage client wrapper used by the consumer to persist raw events."""

import os

from minio import Minio

# Import the actual write/read helpers from the shared storage module
import sys
sys.path.append("/app/storage")
from minio_writer import save_events_to_minio  # noqa: E402


class StorageClient:
    def __init__(self):
        host = os.getenv("MINIO_HOST", "minio")
        port = int(os.getenv("MINIO_PORT", 9000))
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
        self.bucket = os.getenv("MINIO_BUCKET", "signal-catcher-bucket")

        self.client = Minio(
            f"{host}:{port}",
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

        # Create bucket if it doesn't exist
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            print(f"[StorageClient] Created bucket: {self.bucket}")

    def save_batch(self, events_list: list[dict]):
        if not events_list:
            return
        save_events_to_minio(events_list, self.client, self.bucket)
        print(f"[StorageClient] Saved {len(events_list)} events to MinIO")
