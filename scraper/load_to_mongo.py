"""
DEPRECATED: This module is no longer part of the main pipeline.
Use scraper/producer.py and consumers/mongo_consumer.py instead for a distributed queue-based approach.

This file is kept for backward compatibility and direct MongoDB insertion for one-off tasks.
The modern approach uses Kafka as the message queue for better scalability and fault tolerance.
"""

import os
import logging

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

from scraper.rss_fetcher import fetch_all_feeds

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "news_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "raw_articles")


def get_mongo_collection():
    """Get MongoDB collection with retry logic."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")

        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        collection.create_index("article_id", unique=True)
        collection.create_index("source")
        collection.create_index("published_at")

        logger.info("Connected to MongoDB")
        return client, collection
    except PyMongoError as e:
        logger.error("Failed to connect to MongoDB: %s", e)
        raise


def upsert_articles():
    """
    Direct MongoDB insertion (DEPRECATED).
    
    This is a backup method for direct insertion.
    Use the Kafka-based pipeline (producer -> consumer) for production.
    """
    logger.warning("DEPRECATED: Using direct MongoDB insertion. Use Kafka pipeline instead.")
    
    try:
        records = fetch_all_feeds()

        if not records:
            logger.warning("No records fetched. Nothing to load.")
            return

        client, collection = get_mongo_collection()

        try:
            operations = [
                UpdateOne(
                    {"article_id": record["article_id"]},
                    {"$set": record},
                    upsert=True,
                )
                for record in records
            ]

            result = collection.bulk_write(operations, ordered=False)

            logger.info("MongoDB upsert completed")
            logger.info("Inserted: %d", result.upserted_count)
            logger.info("Modified: %d", result.modified_count)
            logger.info("Matched: %d", result.matched_count)
        finally:
            client.close()
    except Exception as e:
        logger.error("Error upserting articles: %s", e)
        raise


if __name__ == "__main__":
    logger.warning("Running deprecated direct MongoDB insertion method")
    upsert_articles()