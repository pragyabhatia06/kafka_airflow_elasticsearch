import json
import os
import logging
import time
from typing import Any, Dict

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rss_news_raw")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "mongo-news-consumer-group")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "news_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "raw_articles")

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def get_collection(attempt: int = 1):
    """Get MongoDB collection with retry logic."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")

        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        collection.create_index("article_id", unique=True)
        collection.create_index("source")
        collection.create_index("published_at")

        logger.info("Connected to MongoDB: %s", MONGO_DB)
        return client, collection
    except PyMongoError as e:
        if attempt < MAX_RETRIES:
            logger.warning(
                "Failed to connect to MongoDB (attempt %d/%d): %s. Retrying in %d seconds...",
                attempt,
                MAX_RETRIES,
                e,
                RETRY_DELAY,
            )
            time.sleep(RETRY_DELAY)
            return get_collection(attempt + 1)
        else:
            logger.error(
                "Failed to connect to MongoDB after %d attempts: %s",
                MAX_RETRIES,
                e,
            )
            raise


def run_consumer():
    """Consume messages from Kafka and write to MongoDB."""
    client = None
    collection = None

    try:
        # Initialize MongoDB connection
        client, collection = get_collection()

        # Create Kafka consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )

        logger.info("Listening to Kafka topic: %s", KAFKA_TOPIC)
        processed_count = 0

        for message in consumer:
            try:
                record: Dict[str, Any] = message.value

                operation = UpdateOne(
                    {"article_id": record["article_id"]},
                    {"$set": record},
                    upsert=True,
                )

                result = collection.bulk_write([operation], ordered=False)

                logger.info(
                    "Processed article_id=%s inserted=%s modified=%s matched=%s",
                    record["article_id"],
                    result.upserted_count,
                    result.modified_count,
                    result.matched_count,
                )
                processed_count += 1

            except PyMongoError as e:
                logger.error("MongoDB write error for message offset %s: %s", message.offset, e)
            except Exception as e:
                logger.error("Error processing message offset %s: %s", message.offset, e)

    except KafkaError as e:
        logger.error("Kafka error: %s", e)
    except PyMongoError as e:
        logger.error("MongoDB error: %s", e)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error("Unexpected error in consumer: %s", e)
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")


if __name__ == "__main__":
    run_consumer()