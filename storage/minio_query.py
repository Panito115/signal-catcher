"""MinIO read helper — queries stored events for a given date.

Usage example (run directly for quick ad-hoc queries):
  python minio_query.py conversion 2026 05 10
"""

import io
import json
import os
import sys

from minio import Minio


def get_minio_client() -> Minio:
    host = os.getenv("MINIO_HOST", "minio")
    port = int(os.getenv("MINIO_PORT", 9000))
    return Minio(
        f"{host}:{port}",
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )


def query_events_by_date(
    minio_client: Minio,
    bucket_name: str,
    event_type: str,
    year: int,
    month: int,
    day: int,
) -> list[dict]:
    prefix = (
        f"events/{event_type}"
        f"/year={year}"
        f"/month={month:02d}"
        f"/day={day:02d}/"
    )

    all_events: list[dict] = []
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)

    for obj in objects:
        response = minio_client.get_object(bucket_name, obj.object_name)
        try:
            raw = response.read()
            events = json.loads(raw.decode("utf-8"))
            all_events.extend(events)
        finally:
            response.close()
            response.release_conn()

    total_count = len(all_events)
    print(f"[Query] {event_type} events on {year}-{month:02d}-{day:02d}: {total_count} total")

    if event_type == "conversion":
        total_revenue = sum(e.get("conversion_value", 0.0) for e in all_events)
        print(f"[Query] Total revenue: {total_revenue:.2f}")

    return all_events


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python minio_query.py <event_type> <year> <month> <day>")
        sys.exit(1)

    _, etype, yr, mo, dy = sys.argv
    client = get_minio_client()
    bucket = os.getenv("MINIO_BUCKET", "signal-catcher-bucket")
    results = query_events_by_date(client, bucket, etype, int(yr), int(mo), int(dy))
    print(f"Returned {len(results)} events")
