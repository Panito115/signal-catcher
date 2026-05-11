"""RabbitMQ client used by the API to publish events to exchanges."""

import json
import os
import time
import pika


class RabbitMQClient:
    def __init__(self):
        self.host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self.port = int(os.getenv("RABBITMQ_PORT", 5672))
        self.user = os.getenv("RABBITMQ_USER", "guest")
        self.password = os.getenv("RABBITMQ_PASS", "guest")
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        # Retry up to 5 times — RabbitMQ needs a few seconds to start
        for attempt in range(1, 6):
            try:
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                print(f"[RabbitMQ] Connected on attempt {attempt}")
                return
            except Exception as exc:
                print(f"[RabbitMQ] Connection attempt {attempt} failed: {exc}")
                if attempt < 5:
                    time.sleep(2)
        raise RuntimeError("Could not connect to RabbitMQ after 5 attempts")

    def declare_exchanges(self):
        # Main event exchanges (direct, durable)
        for exchange in ("events.impressions", "events.clicks", "events.conversions"):
            self.channel.exchange_declare(
                exchange=exchange, exchange_type="direct", durable=True
            )

        # Dead-letter exchange + queue for unprocessable messages
        self.channel.exchange_declare(
            exchange="events.dlx", exchange_type="direct", durable=True
        )
        self.channel.queue_declare(queue="dlq.queue", durable=True)
        self.channel.queue_bind(
            queue="dlq.queue", exchange="events.dlx", routing_key="dlq"
        )

        # Consumer queues bound to their respective exchanges
        queue_bindings = [
            ("impressions.queue", "events.impressions", ""),
            ("clicks.queue", "events.clicks", ""),
            ("conversions.queue", "events.conversions", ""),
        ]
        for queue, exchange, routing_key in queue_bindings:
            self.channel.queue_declare(
                queue=queue,
                durable=True,
                arguments={"x-dead-letter-exchange": "events.dlx", "x-dead-letter-routing-key": "dlq"},
            )
            self.channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    def publish(self, exchange: str, message_dict: dict):
        body = json.dumps(message_dict).encode()
        self.channel.basic_publish(
            exchange=exchange,
            routing_key="",
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
