import json
import logging
import os
import time
from typing import Any, Dict

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rss_news_raw")
ELASTIC_CONSUMER_GROUP = os.getenv("ELASTIC_CONSUMER_GROUP", "elastic-news-consumer-group")

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "news_articles")
MAX_ES_RETRIES = 20
ES_RETRY_DELAY_SECONDS = 3


def get_es_client() -> Elasticsearch:
    last_error: Exception | None = None

    for attempt in range(1, MAX_ES_RETRIES + 1):
        try:
            es = Elasticsearch(ELASTICSEARCH_HOST)
            es.info()
            logger.info("Connected to Elasticsearch at %s", ELASTICSEARCH_HOST)
            return es
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Elasticsearch not ready (attempt %d/%d): %s. Retrying in %ds...",
                attempt,
                MAX_ES_RETRIES,
                exc,
                ES_RETRY_DELAY_SECONDS,
            )
            time.sleep(ES_RETRY_DELAY_SECONDS)

    raise ConnectionError(
        f"Could not connect to Elasticsearch at {ELASTICSEARCH_HOST} after {MAX_ES_RETRIES} attempts"
    ) from last_error


def ensure_index(es: Elasticsearch) -> None:
    if es.indices.exists(index=ELASTICSEARCH_INDEX):
        return

    mapping = {
        "mappings": {
            "properties": {
                "article_id": {"type": "keyword"},
                "source": {"type": "keyword"},
                "title": {"type": "text"},
                "link": {"type": "keyword"},
                "published_at": {"type": "text"},
                "summary": {"type": "text"},
                "loaded_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"}
            }
        }
    }

    es.indices.create(index=ELASTICSEARCH_INDEX, body=mapping)
    logger.info("Created Elasticsearch index: %s", ELASTICSEARCH_INDEX)


def normalize_record_for_elastic(record: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(record)
    loaded_at = normalized.get("loaded_at")

    if isinstance(loaded_at, str) and " " in loaded_at and "T" not in loaded_at:
        # Convert 'YYYY-MM-DD HH:MM:SS+00:00' to ISO-8601 expected by ES date mapper.
        normalized["loaded_at"] = loaded_at.replace(" ", "T", 1)

    return normalized


def run_consumer():
    es = get_es_client()
    ensure_index(es)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=ELASTIC_CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info("Listening to Kafka topic '%s' for Elasticsearch indexing", KAFKA_TOPIC)

    try:
        for message in consumer:
            record: Dict[str, Any] = message.value
            article_id = record["article_id"]

            try:
                document = normalize_record_for_elastic(record)
                es.index(
                    index=ELASTICSEARCH_INDEX,
                    id=article_id,
                    document=document,
                )
                logger.info("Indexed article_id=%s into Elasticsearch", article_id)
            except Exception as exc:
                logger.error("Failed to index article_id=%s: %s", article_id, exc)

    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()