import pika
import json
import os
import time
import threading
from dotenv import load_dotenv

load_dotenv()

# Thread-local storage: each worker gets its own connection
_local = threading.local()

def get_channel():
    """Get or create a RabbitMQ channel for the current thread/worker."""
    if not hasattr(_local, 'channel') or _local.channel is None or _local.channel.is_closed:
        _local.connection = _create_connection()
        _local.channel = _local.connection.channel()
        _declare_topology(_local.channel)
    return _local.channel

def _create_connection():
    host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    port = int(os.getenv("RABBITMQ_PORT", 5672))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASS", "guest")
    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    for attempt in range(1, 6):
        try:
            print(f"[RabbitMQ] Connecting attempt {attempt}...")
            conn = pika.BlockingConnection(parameters)
            print(f"[RabbitMQ] Connected on attempt {attempt}")
            return conn
        except Exception as e:
            print(f"[RabbitMQ] Attempt {attempt} failed: {e}")
            time.sleep(2)
    raise RuntimeError("Could not connect to RabbitMQ after 5 attempts")

def _declare_topology(channel):
    """Declare exchanges, queues, and bindings.

    IMPORTANT: queue arguments declared here MUST be identical to those in
    consumer/consumer.py. RabbitMQ forbids re-declaring a queue with different
    arguments (raises PRECONDITION_FAILED), which would crash the consumer.
    """
    exchanges = ['events.impressions', 'events.clicks', 'events.conversions']
    queues    = ['impressions.queue',  'clicks.queue',  'conversions.queue']

    # DLX exchange and DLQ
    channel.exchange_declare(exchange='events.dlx', exchange_type='direct', durable=True)
    channel.queue_declare(queue='dlq.queue', durable=True)
    # FIX: bind the DLQ so dead-lettered messages actually arrive here.
    # Without this binding the DLQ queue exists but is never reachable from events.dlx.
    channel.queue_bind(queue='dlq.queue', exchange='events.dlx', routing_key='dlq')

    for exchange, queue in zip(exchanges, queues):
        channel.exchange_declare(exchange=exchange, exchange_type='direct', durable=True)
        channel.queue_declare(
            queue=queue,
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'events.dlx',
                # FIX: must match the dlq.queue binding key above.
                # Without this, RabbitMQ uses the original routing key when
                # dead-lettering (e.g. "events.impressions"), which does not
                # match the "dlq" binding — messages are silently dropped.
                'x-dead-letter-routing-key': 'dlq',
            }
        )
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=exchange)

def publish(exchange: str, message: dict):
    """Publish a message. Reconnects automatically if channel is closed."""
    for attempt in range(3):
        try:
            channel = get_channel()
            channel.basic_publish(
                exchange=exchange,
                routing_key=exchange,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            return
        except Exception as e:
            print(f"[RabbitMQ] Publish failed (attempt {attempt+1}): {e}. Reconnecting...")
            _local.channel = None
            if hasattr(_local, 'connection'):
                try:
                    _local.connection.close()
                except:
                    pass
            time.sleep(0.1)
    raise RuntimeError(f"Failed to publish to {exchange} after 3 attempts")