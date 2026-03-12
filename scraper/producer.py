import json
import os
import logging
import time
from typing import Any, Dict, List

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

from scraper.rss_fetcher import fetch_all_feeds

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rss_news_raw")
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def get_producer(attempt: int = 1) -> KafkaProducer:
    """Create a Kafka producer with retry logic."""
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            retries=5,
            acks="all",
        )
    except KafkaError as e:
        if attempt < MAX_RETRIES:
            logger.warning(
                "Failed to connect to Kafka (attempt %d/%d): %s. Retrying in %d seconds...",
                attempt,
                MAX_RETRIES,
                e,
                RETRY_DELAY,
            )
            time.sleep(RETRY_DELAY)
            return get_producer(attempt + 1)
        else:
            logger.error(
                "Failed to connect to Kafka after %d attempts: %s",
                MAX_RETRIES,
                e,
            )
            raise


def publish_articles() -> None:
    """Fetch RSS articles and publish them to Kafka."""
    try:
        records: List[Dict[str, Any]] = fetch_all_feeds()

        if not records:
            logger.warning("No records fetched. Nothing to publish.")
            return

        producer = get_producer()

        try:
            published_count = 0
            failed_count = 0

            for record in records:
                try:
                    future = producer.send(
                        KAFKA_TOPIC,
                        key=record["article_id"],
                        value=record,
                    )
                    # Wait for the send to complete
                    future.get(timeout=10)
                    published_count += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish article %s: %s",
                        record.get("article_id"),
                        e,
                    )
                    failed_count += 1

            producer.flush()
            logger.info(
                "Published %d/%d records to topic %s (%d failed)",
                published_count,
                len(records),
                KAFKA_TOPIC,
                failed_count,
            )

            if failed_count > 0:
                raise Exception(
                    f"Failed to publish {failed_count} out of {len(records)} records"
                )

        finally:
            producer.close()

    except Exception as e:
        logger.error("Error in publish_articles: %s", e)
        raise


if __name__ == "__main__":
    publish_articles()