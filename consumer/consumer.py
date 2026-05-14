"""Main consumer process.

Subscribes to the three RabbitMQ queues, accumulates events in memory, and
flushes aggregated metrics to InfluxDB + raw events to MinIO every
BATCH_INTERVAL_SECONDS seconds.
"""

import json
import os
import threading
import time

import pika

from aggregator import EventAggregator
from influx_writer import InfluxWriter
from storage_client import StorageClient

BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL_SECONDS", 5))
QUEUES = ["impressions.queue", "clicks.queue", "conversions.queue"]
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# RabbitMQ connection with exponential back-off
# ---------------------------------------------------------------------------

def connect_rabbitmq() -> pika.BlockingConnection:
    host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    port = int(os.getenv("RABBITMQ_PORT", 5672))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASS", "guest")

    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    delay = 1
    for attempt in range(1, 4):
        try:
            conn = pika.BlockingConnection(params)
            print(f"[Consumer] Connected to RabbitMQ on attempt {attempt}")
            return conn
        except Exception as exc:
            print(f"[Consumer] RabbitMQ attempt {attempt} failed: {exc}")
            if attempt < 3:
                time.sleep(delay)
                delay *= 2
    raise RuntimeError("Could not connect to RabbitMQ after 3 attempts")


# ---------------------------------------------------------------------------
# Flush loop — runs in a background thread
# ---------------------------------------------------------------------------

def flush_loop(aggregator: EventAggregator, influx: InfluxWriter, storage: StorageClient, raw_buffer: list, raw_lock: threading.Lock):
    while True:
        time.sleep(BATCH_INTERVAL)
        try:
            metrics = aggregator.flush()
            if metrics:
                influx.write_batch(metrics)

            with raw_lock:
                batch = list(raw_buffer)
                raw_buffer.clear()

            if batch:
                storage.save_batch(batch)
        except Exception as exc:
            print(f"[FlushLoop] Error during flush: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    aggregator = EventAggregator()
    influx = InfluxWriter()
    storage = StorageClient()

    raw_buffer: list[dict] = []
    raw_lock = threading.Lock()

    flush_thread = threading.Thread(
        target=flush_loop,
        args=(aggregator, influx, storage, raw_buffer, raw_lock),
        daemon=True,
    )
    flush_thread.start()

    connection = connect_rabbitmq()
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)

    # Exchanges
    for exchange in ("events.impressions", "events.clicks", "events.conversions"):
        channel.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange="events.dlx", exchange_type="direct", durable=True)

    # Queues — failed messages dead-letter to events.dlx with routing key 'dlq'
    for queue, exchange in [
        ("impressions.queue", "events.impressions"),
        ("clicks.queue", "events.clicks"),
        ("conversions.queue", "events.conversions"),
    ]:
        channel.queue_declare(
            queue=queue,
            durable=True,
            arguments={"x-dead-letter-exchange": "events.dlx"},
        )
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=exchange)

    channel.queue_declare(queue="dlq.queue", durable=True)
    channel.queue_bind(queue="dlq.queue", exchange="events.dlx", routing_key="dlq")

    def on_message(ch, method, properties, body):
        try:
            event = json.loads(body)
            payload = event.get('payload', {})
            if not isinstance(payload, dict):
                raise ValueError(f"Invalid payload type: {type(payload)}. Expected dict.")
            aggregator.add_event(event)
            with raw_lock:
                raw_buffer.append(event)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            headers = dict(properties.headers or {})
            retry_count = int(headers.get("x-retry-count", 0))

            if retry_count < MAX_RETRIES:
                wait = 2 ** retry_count  # 1s, 2s, 4s
                print(
                    f"[Consumer] Processing failed (attempt {retry_count + 1}/{MAX_RETRIES}), "
                    f"retrying in {wait}s: {exc}"
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                time.sleep(wait)
                headers["x-retry-count"] = retry_count + 1
                ch.basic_publish(
                    exchange=method.exchange,
                    routing_key=method.routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        headers=headers,
                    ),
                )
            else:
                print(f"[Consumer] Max retries reached, routing to DLQ: {exc}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    for queue in QUEUES:
        channel.basic_consume(queue=queue, on_message_callback=on_message)

    print(f"[Consumer] Listening on queues: {QUEUES}")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()
        influx.close()


if __name__ == "__main__":
    main()
