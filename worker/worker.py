import json
import logging
import os
import time
import urllib.error
import urllib.request

import pika


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("agent-worker")
QUEUE = "tasks.logical"
RETRY_QUEUE = "tasks.logical.retry"
MAX_RETRIES = 3


def report_result(task_id: str, status: str, result: str = "", error: str = "") -> None:
    api_url = os.getenv("API_INTERNAL_URL", "http://api:8080")
    request_body = json.dumps({"status": status, "result": result, "error": error}).encode()
    headers = {"Content-Type": "application/json"}
    token = os.getenv("INTERNAL_TOKEN", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"{api_url}/internal/tasks/{task_id}/result",
        data=request_body,
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        if response.status not in (200, 204):
            raise RuntimeError(f"result callback returned HTTP {response.status}")


def process_task(payload: dict) -> None:
    prompt = payload["prompt"]
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("task prompt is empty")
    report_result(
        payload["task_id"],
        "completed",
        "Worker placeholder received the request. Connect a model provider to generate a response.",
    )


def on_message(channel, method, properties, body: bytes) -> None:
    try:
        payload = json.loads(body)
        process_task(payload)
    except (ValueError, KeyError, json.JSONDecodeError) as exc:
        LOGGER.exception("invalid task message")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    except (urllib.error.URLError, TimeoutError, RuntimeError) as exc:
        LOGGER.warning("task callback failed; requeueing: %s", exc)
        retries = (properties.headers or {}).get("x-retry-count", 0)
        if retries < MAX_RETRIES:
            headers = dict(properties.headers or {})
            headers["x-retry-count"] = retries + 1
            try:
                channel.basic_publish(
                    exchange="",
                    routing_key=RETRY_QUEUE,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type=properties.content_type,
                        delivery_mode=pika.DeliveryMode.Persistent,
                        message_id=properties.message_id,
                        headers=headers,
                    ),
                    mandatory=True,
                )
            except pika.exceptions.AMQPError:
                LOGGER.exception("could not publish retry")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            else:
                channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            LOGGER.error("task %s exceeded retry limit", properties.message_id)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    broker_url = os.getenv("RABBITMQ_URL", "amqp://orchestrator:orchestrator@rabbitmq:5672/")
    while True:
        try:
            parameters = pika.URLParameters(broker_url)
            parameters.heartbeat = 30
            parameters.blocked_connection_timeout = 10
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare(exchange="tasks.dead", exchange_type="fanout", durable=True)
            channel.queue_declare(queue="tasks.dead", durable=True)
            channel.queue_bind(queue="tasks.dead", exchange="tasks.dead")
            channel.queue_declare(
                queue=RETRY_QUEUE,
                durable=True,
                arguments={
                    "x-message-ttl": 2000,
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": QUEUE,
                },
            )
            channel.queue_declare(
                queue=QUEUE,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-dead-letter-exchange": "tasks.dead",
                    "x-dead-letter-routing-key": "",
                },
            )
            channel.confirm_delivery()
            channel.basic_qos(prefetch_count=8)
            channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
            LOGGER.info("worker consuming %s", QUEUE)
            channel.start_consuming()
        except pika.exceptions.AMQPError:
            LOGGER.exception("RabbitMQ connection failed")
            time.sleep(2)


if __name__ == "__main__":
    main()