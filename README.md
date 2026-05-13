# signal-catcher

Real-time ad tracking analytics system — ingests impression, click, and conversion events via a REST API, processes them through a message queue, and visualises aggregated metrics in Grafana.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose v2)

## Quick start

```bash
git clone <repo-url>
cd signal-catcher
cp .env.example .env
docker compose up --build
```

## Service URLs

| Service         | URL                        | Credentials              |
|-----------------|----------------------------|--------------------------|
| API             | http://localhost:8000      | —                        |
| API docs        | http://localhost:8000/docs | —                        |
| RabbitMQ UI     | http://localhost:15672     | guest / guest            |
| InfluxDB        | http://localhost:8086      | admin / admin123         |
| MinIO Console   | http://localhost:9001      | minioadmin / minioadmin123 |
| Grafana         | http://localhost:3000      | admin / admin            |

## How to test

Send a sample impression event:

```bash
curl -X POST http://localhost:8000/api/events/impression \
  -H "Content-Type: application/json" \
  -d '{
    "impression_id": "imp-001",
    "user_ip": "192.168.1.10",
    "user_agent": "Mozilla/5.0",
    "timestamp": "2026-05-10T12:00:00Z",
    "state": "CA",
    "search_keywords": "running shoes",
    "session_id": "sess-abc123",
    "ads": [
      {
        "advertiser": { "advertiser_id": "adv-1", "advertiser_name": "Nike" },
        "campaign": { "campaign_id": "camp-1", "campaign_name": "Summer Sale" },
        "ad": {
          "ad_id": "ad-1",
          "ad_name": "Nike Air Max",
          "ad_text": "Run faster. Buy now.",
          "ad_link": "https://nike.com/airmax",
          "ad_position": 1,
          "ad_format": "banner"
        }
      }
    ]
  }'
```

Health check:

```bash
curl http://localhost:8000/health
```

## Scale consumers

```bash
docker compose up --scale consumer=5 -d
```

## Load testing

Install [Artillery](https://www.artillery.io/) and run:

```bash
npm install -g artillery
artillery run load_test.yml
```

## Project structure

```
signal-catcher/
├── api/          FastAPI ingestion service (POST events → RabbitMQ)
├── consumer/     Event consumer + aggregator (RabbitMQ → InfluxDB + MinIO)
├── storage/      MinIO read/write helpers
└── grafana/      Pre-provisioned datasource and dashboard
```
