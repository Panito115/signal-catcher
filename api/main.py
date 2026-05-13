"""FastAPI ingestion service — receives ad events and pushes them to RabbitMQ.

All endpoints must respond in < 50 ms. No processing happens here; work is
delegated to the consumer service via the message queue.
"""

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from models import ClickEvent, ConversionEvent, ImpressionEvent
from queue_client import publish

app = FastAPI(title="Signal Catcher API")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/events/impression", status_code=202)
def ingest_impression(event: ImpressionEvent):
    payload = event.model_dump()
    payload["event_type"] = "impression"
    payload["received_at"] = _now_iso()
    publish("events.impressions", payload)
    return JSONResponse(
        {"status": "accepted", "event_id": event.impression_id}, status_code=202
    )


@app.post("/api/events/click", status_code=202)
def ingest_click(event: ClickEvent):
    payload = event.model_dump()
    payload["event_type"] = "click"
    payload["received_at"] = _now_iso()
    publish("events.clicks", payload)
    return JSONResponse(
        {"status": "accepted", "event_id": event.click_id}, status_code=202
    )


@app.post("/api/events/conversion", status_code=202)
def ingest_conversion(event: ConversionEvent):
    payload = event.model_dump()
    payload["event_type"] = "conversion"
    payload["received_at"] = _now_iso()
    publish("events.conversions", payload)
    return JSONResponse(
        {"status": "accepted", "event_id": event.conversion_id}, status_code=202
    )
