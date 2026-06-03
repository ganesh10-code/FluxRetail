"""
Kafka producer for retail events.

Uses confluent-kafka for production-grade delivery guarantees:
  - JSON-serialised RetailEvent messages
  - Keyed by visitor_id for partition locality
  - Delivery report callback for observability
  - Configurable retry on startup until Kafka is ready
"""

from __future__ import annotations

import json
import time
from typing import Callable

import structlog
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from config import Settings
from models import RetailEvent

logger = structlog.get_logger(__name__)


class RetailEventProducer:
    """
    Kafka producer that publishes RetailEvent objects.

    Usage:
        producer = RetailEventProducer(settings)
        producer.connect()  # waits for Kafka to be ready
        producer.publish(event)
        producer.flush()    # call periodically or on shutdown
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: Producer | None = None

    def connect(self) -> None:
        """
        Connect to Kafka and ensure required topics exist.
        Retries until `kafka_producer_timeout_seconds` elapses.
        """
        kafka_config = {
            "bootstrap.servers": self._settings.kafka_bootstrap_servers,
            "client.id": f"fluxretail-pipeline-{self._settings.store_id}",
            "acks": "1",  # leader ack — balance durability vs. throughput
            "retries": 3,
            "retry.backoff.ms": 500,
            "linger.ms": 5,  # small batching window
            "compression.type": "lz4",
        }

        deadline = time.monotonic() + self._settings.kafka_producer_timeout_seconds
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            try:
                self._producer = Producer(kafka_config)
                # Verify connectivity by listing topics
                admin = AdminClient({"bootstrap.servers": self._settings.kafka_bootstrap_servers})
                meta = admin.list_topics(timeout=5)
                self._ensure_topics(admin)
                logger.info(
                    "kafka_producer_connected",
                    brokers=self._settings.kafka_bootstrap_servers,
                    topics=list(meta.topics.keys()),
                )
                return
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "kafka_not_ready_retrying",
                    error=str(exc),
                    retry_in_seconds=5,
                )
                time.sleep(5)

        raise RuntimeError(
            f"Failed to connect to Kafka after {self._settings.kafka_producer_timeout_seconds}s: {last_error}"
        )

    def publish(self, event: RetailEvent) -> None:
        """Publish a single RetailEvent to the events topic."""
        if self._producer is None:
            raise RuntimeError("RetailEventProducer.connect() must be called first")

        payload = json.dumps(event.to_kafka_dict()).encode("utf-8")
        key = event.visitor_id.encode("utf-8")

        self._producer.produce(
            topic=self._settings.kafka_events_topic,
            key=key,
            value=payload,
            callback=self._delivery_report,
        )
        # Poll to trigger delivery callbacks (non-blocking)
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> None:
        """Flush all buffered messages. Call at end of frame batch."""
        if self._producer:
            self._producer.flush(timeout=timeout)

    def _delivery_report(self, err, msg) -> None:
        if err:
            logger.error(
                "kafka_delivery_failed",
                topic=msg.topic(),
                error=str(err),
            )
        else:
            logger.debug(
                "kafka_delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def _ensure_topics(self, admin: AdminClient) -> None:
        """Create Kafka topics if they don't exist."""
        existing = admin.list_topics(timeout=5).topics
        topics_to_create = []
        for topic_name in [
            self._settings.kafka_events_topic,
            self._settings.kafka_metrics_topic,
            self._settings.kafka_alerts_topic,
        ]:
            if topic_name not in existing:
                topics_to_create.append(
                    NewTopic(topic_name, num_partitions=1, replication_factor=1)
                )

        if topics_to_create:
            fs = admin.create_topics(topics_to_create)
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info("kafka_topic_created", topic=topic)
                except Exception as exc:
                    logger.warning("kafka_topic_creation_skipped", topic=topic, reason=str(exc))
