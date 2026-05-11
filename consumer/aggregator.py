"""In-memory event aggregator.

Buffers raw events and, on flush(), computes per-group metrics ready to be
written to InfluxDB. Thread-safe via a Lock so the flush loop (separate
thread) and the consumer callback can both access the buffer safely.
"""

import threading
from collections import defaultdict
from typing import Any


class EventAggregator:
    def __init__(self):
        # key: (event_type, state, advertiser_id, campaign_id, ad_id, search_keyword)
        # value: dict of accumulated counters
        self._buffer: dict[tuple, dict[str, Any]] = defaultdict(
            lambda: {
                "impressions": 0,
                "clicks": 0,
                "conversions": 0,
                "revenue": 0.0,
                "time_to_click_sum": 0.0,
                "time_to_click_count": 0,
                "time_to_convert_sum": 0,
                "time_to_convert_count": 0,
            }
        )
        self._lock = threading.Lock()

    def add_event(self, event_dict: dict):
        event_type = event_dict.get("event_type", "unknown")

        # Extract dimensional keys depending on event type
        if event_type == "impression":
            state = event_dict.get("state", "")
            search_keyword = event_dict.get("search_keywords", "")
            first_ad = (event_dict.get("ads") or [{}])[0]
            advertiser_id = first_ad.get("advertiser", {}).get("advertiser_id", "")
            campaign_id = first_ad.get("campaign", {}).get("campaign_id", "")
            ad_id = first_ad.get("ad", {}).get("ad_id", "")
        elif event_type == "click":
            state = event_dict.get("user_info", {}).get("state", "")
            search_keyword = ""
            clicked_ad = event_dict.get("clicked_ad", {})
            ad_id = clicked_ad.get("ad_id", "")
            advertiser_id = ""
            campaign_id = ""
        elif event_type == "conversion":
            state = event_dict.get("user_info", {}).get("state", "")
            search_keyword = ""
            ad_id = ""
            advertiser_id = ""
            campaign_id = ""
        else:
            return

        group_key = (event_type, state, advertiser_id, campaign_id, ad_id, search_keyword)

        with self._lock:
            bucket = self._buffer[group_key]
            if event_type == "impression":
                bucket["impressions"] += 1
            elif event_type == "click":
                bucket["clicks"] += 1
                time_to_click = event_dict.get("clicked_ad", {}).get("time_to_click", 0.0)
                bucket["time_to_click_sum"] += time_to_click
                bucket["time_to_click_count"] += 1
            elif event_type == "conversion":
                bucket["conversions"] += 1
                bucket["revenue"] += event_dict.get("conversion_value", 0.0)
                time_to_convert = event_dict.get("attribution_info", {}).get("time_to_convert", 0)
                bucket["time_to_convert_sum"] += time_to_convert
                bucket["time_to_convert_count"] += 1

    def flush(self) -> list[dict]:
        with self._lock:
            snapshot = dict(self._buffer)
            self._buffer.clear()

        # Aggregate per-ad_id impression/click counts for CTR/conversion rate
        # Build a secondary index: ad_id -> {impressions, clicks, conversions}
        ad_totals: dict[str, dict[str, int]] = defaultdict(
            lambda: {"impressions": 0, "clicks": 0, "conversions": 0}
        )
        for (event_type, state, advertiser_id, campaign_id, ad_id, kw), counts in snapshot.items():
            ad_totals[ad_id]["impressions"] += counts["impressions"]
            ad_totals[ad_id]["clicks"] += counts["clicks"]
            ad_totals[ad_id]["conversions"] += counts["conversions"]

        metrics = []
        for (event_type, state, advertiser_id, campaign_id, ad_id, kw), counts in snapshot.items():
            totals = ad_totals[ad_id]
            impressions = totals["impressions"]
            clicks = totals["clicks"]
            conversions = totals["conversions"]

            ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
            conversion_rate = (conversions / clicks * 100) if clicks > 0 else 0.0

            avg_time_to_click = (
                counts["time_to_click_sum"] / counts["time_to_click_count"]
                if counts["time_to_click_count"] > 0
                else 0.0
            )
            avg_time_to_convert = (
                counts["time_to_convert_sum"] / counts["time_to_convert_count"]
                if counts["time_to_convert_count"] > 0
                else 0.0
            )

            count = (
                counts["impressions"]
                or counts["clicks"]
                or counts["conversions"]
            )

            metrics.append(
                {
                    "event_type": event_type,
                    "state": state,
                    "advertiser_id": advertiser_id,
                    "campaign_id": campaign_id,
                    "ad_id": ad_id,
                    "search_keyword": kw,
                    "count": count,
                    "revenue": counts["revenue"],
                    "ctr": ctr,
                    "conversion_rate": conversion_rate,
                    "avg_time_to_click": avg_time_to_click,
                    "avg_time_to_convert": avg_time_to_convert,
                }
            )
        return metrics
