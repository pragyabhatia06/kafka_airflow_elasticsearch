import logging
import os
from datetime import datetime
from typing import Any, Dict, List

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "news_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "raw_articles")

app = Flask(__name__)


def get_collection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        return client, collection
    except PyMongoError as e:
        logger.error("Failed to connect to MongoDB: %s", e)
        raise


def serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "article_id": doc.get("article_id"),
        "source": doc.get("source"),
        "title": doc.get("title"),
        "link": doc.get("link"),
        "published_at": doc.get("published_at"),
        "summary": doc.get("summary"),
        "loaded_at": doc.get("loaded_at").isoformat()
        if isinstance(doc.get("loaded_at"), datetime)
        else doc.get("loaded_at"),
    }


@app.route("/health", methods=["GET"])
def health():
    try:
        client, _ = get_collection()
        client.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error("Health check failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/news", methods=["GET"])
def get_news():
    try:
        source = request.args.get("source")
        limit = request.args.get("limit", default=20, type=int)
        limit = min(max(limit, 1), 100)

        query = {}
        if source:
            query["source"] = source

        client, collection = get_collection()
        try:
            cursor = collection.find(query, {"_id": 0}).sort("loaded_at", -1).limit(limit)
            news: List[Dict[str, Any]] = [serialize_doc(doc) for doc in cursor]
            logger.info("Retrieved %d articles (source=%s, limit=%d)", len(news), source, limit)
            return jsonify(news), 200
        finally:
            client.close()
    except PyMongoError as e:
        logger.error("Database error: %s", e)
        return jsonify({"error": "Database error"}), 500
    except Exception as e:
        logger.error("Error retrieving news: %s", e)
        return jsonify({"error": "Internal server error"}), 500


@app.route("/news/<article_id>", methods=["GET"])
def get_news_by_id(article_id: str):
    try:
        client, collection = get_collection()
        try:
            doc = collection.find_one({"article_id": article_id}, {"_id": 0})
            if not doc:
                logger.warning("Article not found: %s", article_id)
                return jsonify({"message": "Article not found"}), 404
            logger.info("Retrieved article: %s", article_id)
            return jsonify(serialize_doc(doc)), 200
        finally:
            client.close()
    except PyMongoError as e:
        logger.error("Database error: %s", e)
        return jsonify({"error": "Database error"}), 500
    except Exception as e:
        logger.error("Error retrieving article: %s", e)
        return jsonify({"error": "Internal server error"}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    logger.error("Internal server error: %s", error)
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)