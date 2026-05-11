"""MinIO write helper — persists raw event batches grouped by type and date.

Path format: events/{event_type}/year=YYYY/month=MM/day=DD/hour=HH/{unix_ts}.json
"""

import io
import json
import time
from datetime import datetime, timezone

from dateutil import parser as dateutil_parser


def save_events_to_minio(events_list: list[dict], minio_client, bucket_name: str):
    if not events_list:
        return

    # Group events by event_type
    groups: dict[str, list[dict]] = {}
    for event in events_list:
        etype = event.get("event_type", "unknown")
        groups.setdefault(etype, []).append(event)

    for event_type, group in groups.items():
        # Use received_at from the first event for the partition path
        received_at_raw = group[0].get("received_at", datetime.now(timezone.utc).isoformat())
        try:
            dt = dateutil_parser.parse(received_at_raw)
        except Exception:
            dt = datetime.now(timezone.utc)

        object_path = (
            f"events/{event_type}"
            f"/year={dt.year}"
            f"/month={dt.month:02d}"
            f"/day={dt.day:02d}"
            f"/hour={dt.hour:02d}"
            f"/{int(time.time())}.json"
        )

        data = json.dumps(group, ensure_ascii=False).encode("utf-8")
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_path,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        print(f"[MinioWriter] Uploaded {len(group)} events → {object_path}")
