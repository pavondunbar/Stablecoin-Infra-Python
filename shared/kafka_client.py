"""
shared/kafka_client.py — Confluent Kafka producer and consumer wrappers.

Design decisions:
  - Producer is a module-level singleton (thread-safe, connection-pooled)
  - All messages are JSON-serialised Pydantic events
  - Producer flushes after each publish for at-least-once delivery semantics
  - Consumer wraps confluent_kafka.Consumer with auto-deserialization
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Callable, Optional, TypeVar

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import BaseModel

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SERVICE_NAME    = os.environ.get("SERVICE_NAME", "unknown-service")

T = TypeVar("T", bound=BaseModel)

# ─── Producer ────────────────────────────────────────────────────────────────

_producer: Optional[Producer] = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "client.id": SERVICE_NAME,
                "acks": "all",                   # wait for all ISR replicas
                "retries": 5,
                "retry.backoff.ms": 300,
                "compression.type": "lz4",
                "enable.idempotence": True,      # exactly-once on the producer side
                "message.timeout.ms": 10000,
            }
        )
    return _producer


def _delivery_report(err, msg):
    if err:
        log.error("Kafka delivery failed | topic=%s err=%s", msg.topic(), err)
    else:
        log.debug(
            "Kafka delivered | topic=%s partition=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def publish(topic: str, event: BaseModel, key: Optional[str] = None) -> None:
    """Publish a Pydantic event to a Kafka topic."""
    producer = _get_producer()
    payload = event.model_dump_json().encode("utf-8")
    producer.produce(
        topic=topic,
        value=payload,
        key=key.encode("utf-8") if key else None,
        on_delivery=_delivery_report,
    )
    producer.flush(timeout=5)
    log.info("Published | topic=%s key=%s", topic, key)


def publish_dict(topic: str, data: dict, key: Optional[str] = None) -> None:
    """Publish a raw dict to a Kafka topic."""
    producer = _get_producer()
    payload = json.dumps(data, default=str).encode("utf-8")
    producer.produce(
        topic=topic,
        value=payload,
        key=key.encode("utf-8") if key else None,
        on_delivery=_delivery_report,
    )
    producer.flush(timeout=5)


# ─── Consumer ────────────────────────────────────────────────────────────────

def build_consumer(group_id: str, topics: list[str]) -> Consumer:
    """Create and subscribe a Kafka consumer."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group_id,
            "client.id": f"{SERVICE_NAME}-{group_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,     # manual commit after processing
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        }
    )
    consumer.subscribe(topics)
    log.info("Consumer subscribed | group=%s topics=%s", group_id, topics)
    return consumer


_retry_tracker: dict[str, int] = {}


def _send_to_dlq(
    dlq_topic: str,
    original_topic: str,
    partition: int,
    offset: int,
    payload: bytes,
    error: str,
    retry_count: int,
) -> None:
    """Publish a failed message to the dead-letter queue topic."""
    dlq_payload = {
        "original_topic": original_topic,
        "original_partition": partition,
        "original_offset": offset,
        "payload": payload.decode("utf-8", errors="replace"),
        "error": str(error),
        "retry_count": retry_count,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
    }
    publish_dict(dlq_topic, dlq_payload)
    log.warning(
        "Sent to DLQ | topic=%s partition=%d offset=%d",
        original_topic, partition, offset,
    )


def consume_loop(
    consumer: Consumer,
    handler: Callable[[str, dict], None],
    poll_timeout: float = 1.0,
    max_errors: int = 10,
    max_handler_retries: int = 3,
    dlq_topic: str = "dlq.default",
) -> None:
    """Blocking consume loop with DLQ routing.

    Calls handler(topic, payload_dict) for each message. Commits
    offset only after successful handler execution. On handler
    failure, retries up to max_handler_retries with exponential
    backoff, then routes to the DLQ topic.
    """
    consecutive_errors = 0
    try:
        while True:
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Consumer error: %s", msg.error())
                consecutive_errors += 1
                if consecutive_errors >= max_errors:
                    raise KafkaException(msg.error())
                time.sleep(1)
                continue

            consecutive_errors = 0
            topic = msg.topic()
            raw = msg.value()
            tracker_key = (
                f"{topic}:{msg.partition()}:{msg.offset()}"
            )
            try:
                payload = json.loads(raw)
                handler(topic, payload)
                consumer.commit(message=msg, asynchronous=False)
                _retry_tracker.pop(tracker_key, None)
            except Exception as exc:
                count = _retry_tracker.get(tracker_key, 0) + 1
                _retry_tracker[tracker_key] = count
                log.exception(
                    "Handler error | topic=%s attempt=%d exc=%s",
                    topic, count, exc,
                )
                if count >= max_handler_retries:
                    _send_to_dlq(
                        dlq_topic, topic,
                        msg.partition(), msg.offset(),
                        raw, str(exc), count,
                    )
                    consumer.commit(
                        message=msg, asynchronous=False
                    )
                    _retry_tracker.pop(tracker_key, None)
                else:
                    backoff = min(2 ** count, 30)
                    time.sleep(backoff)
    finally:
        consumer.close()
        log.info("Consumer closed.")


# ─── Deduplication Helpers ───────────────────────────────────────────────────

def is_duplicate_event(db, event_id: str) -> bool:
    """Check if an event has already been processed."""
    from shared.models import ProcessedEvent
    from sqlalchemy import select

    return db.execute(
        select(ProcessedEvent).where(
            ProcessedEvent.event_id == event_id
        )
    ).scalar_one_or_none() is not None


def mark_event_processed(
    db, event_id: str, topic: str
) -> None:
    """Record that an event has been processed."""
    from shared.models import ProcessedEvent

    db.add(ProcessedEvent(event_id=event_id, topic=topic))
    db.flush()
