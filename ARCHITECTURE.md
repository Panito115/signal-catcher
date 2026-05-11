# Architecture

## Overview

```
Browser / Load-test client
         │  HTTP POST
         ▼
    ┌─────────┐
    │   API   │  FastAPI + Gunicorn (4 workers)
    └────┬────┘
         │ AMQP publish (persistent, durable)
         ▼
    ┌───────────┐
    │ RabbitMQ  │  3 exchanges + 3 queues + 1 DLQ
    └─────┬─────┘
          │ AMQP consume (manual ack, prefetch=1)
          ▼
    ┌──────────────┐   x3 replicas
    │   Consumer   │───────────────────────┐
    └──────┬───────┘                       │
           │ flush every N seconds         │
     ┌─────┴──────┐                        │
     │            │                        │
     ▼            ▼                        ▼
┌─────────┐  ┌─────────┐           ┌──────────┐
│InfluxDB │  │  MinIO  │           │ Grafana  │
│(metrics)│  │  (raw)  │           │(dashbrd) │
└─────────┘  └─────────┘           └──────────┘
```

## Data flow

1. **Ingestion** — API validates the event with Pydantic, enriches it with `event_type` and `received_at`, then publishes to the appropriate RabbitMQ exchange and returns HTTP 202 immediately.

2. **Queuing** — RabbitMQ routes messages to one of three durable queues (`impressions.queue`, `clicks.queue`, `conversions.queue`). Unprocessable messages are routed to `dlq.queue` via the dead-letter exchange.

3. **Processing** — Each consumer replica reads messages, calls `EventAggregator.add_event()` (thread-safe), and ACKs. A background thread flushes every `BATCH_INTERVAL_SECONDS` seconds.

4. **Storage**
   - Aggregated metrics → InfluxDB (measurement: `ad_events`, tags + fields)
   - Raw event JSON → MinIO (`events/{type}/year=…/month=…/day=…/hour=…/{ts}.json`)

5. **Visualisation** — Grafana auto-provisions the InfluxDB datasource and the Signal Catcher dashboard. Refreshes every 10 s.

## Scaling

- Consumers are scaled via `docker compose up --scale consumer=N`.
- RabbitMQ's `prefetch_count=1` ensures fair dispatch across replicas.
- The API scales horizontally behind any load balancer (stateless).

## Dead-letter queue

Messages that fail JSON parsing are nack'd with `requeue=False` and land in `dlq.queue`. Inspect them via the RabbitMQ management UI at `http://localhost:15672`.
