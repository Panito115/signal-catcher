"""Main consumer process.

Subscribes to the three RabbitMQ queues, accumulates events in memory, and
flushes aggregated metrics to InfluxDB + raw events to MinIO every
BATCH_INTERVAL_SECONDS seconds.
"""

import json
import os
import sys
import threading
import time

import pika

from aggregator import EventAggregator
from influx_writer import InfluxWriter
from storage_client import StorageClient

BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL_SECONDS", 5))
QUEUES = ["impressions.queue", "clicks.queue", "conversions.queue"]


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

    # Shared raw-event buffer accessed by both the consumer callback and flush loop
    raw_buffer: list[dict] = []
    raw_lock = threading.Lock()

    # Start flush thread
    flush_thread = threading.Thread(
        target=flush_loop,
        args=(aggregator, influx, storage, raw_buffer, raw_lock),
        daemon=True,
    )
    flush_thread.start()

    connection = connect_rabbitmq()
    channel = connection.channel()

    # Fair dispatch — don't send more than one message to a consumer at a time
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        try:
            event = json.loads(body)
            aggregator.add_event(event)
            with raw_lock:
                raw_buffer.append(event)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except (json.JSONDecodeError, KeyError) as exc:
            print(f"[Consumer] Bad message, sending to DLQ: {exc}")
            # nack without requeue → message goes to dead-letter exchange
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
