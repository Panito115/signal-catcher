"""InfluxDB writer — batch-writes aggregated metrics from the consumer."""

import os
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxWriter:
    def __init__(self):
        url = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
        token = os.getenv("INFLUXDB_TOKEN", "signal-catcher-token-2026")
        self._org = os.getenv("INFLUXDB_ORG", "signal_catcher_org")
        self._bucket = os.getenv("INFLUXDB_BUCKET", "signal_catcher")

        self._client = InfluxDBClient(url=url, token=token, org=self._org)
        # SYNCHRONOUS write_api is safe to use from a background thread
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    def write_batch(self, metrics_list: list[dict]):
        if not metrics_list:
            return

        points = []
        now = datetime.now(timezone.utc)
        for m in metrics_list:
            point = (
                Point("ad_events")
                .tag("event_type", m.get("event_type", ""))
                .tag("state", m.get("state", ""))
                .tag("advertiser_id", m.get("advertiser_id", ""))
                .tag("campaign_id", m.get("campaign_id", ""))
                .tag("ad_id", m.get("ad_id", ""))
                .tag("search_keyword", m.get("search_keyword", ""))
                .field("count", float(m.get("count", 0)))
                .field("revenue", float(m.get("revenue", 0.0)))
                .field("ctr", float(m.get("ctr", 0.0)))
                .field("conversion_rate", float(m.get("conversion_rate", 0.0)))
                .field("avg_time_to_click", float(m.get("avg_time_to_click", 0.0)))
                .field("avg_time_to_convert", float(m.get("avg_time_to_convert", 0.0)))
                .time(now, WritePrecision.NANOSECONDS)
            )
            points.append(point)

        self._write_api.write(bucket=self._bucket, org=self._org, record=points)
        print(f"[InfluxWriter] Wrote {len(points)} points")

    def close(self):
        self._write_api.close()
        self._client.close()
