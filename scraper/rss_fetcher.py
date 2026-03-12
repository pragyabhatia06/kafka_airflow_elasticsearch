import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import feedparser
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RSS_SOURCES = [
    {"name": "BBC", "url": "https://feeds.bbci.co.uk/news/rss.xml"},
    # {"name": "CNN", "url": "https://rss.cnn.com/rss/edition.rss"},
    {"name": "Sky News", "url": "https://feeds.skynews.com/feeds/rss/home.xml"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


def generate_article_id(link: str) -> str:
    """Generate a unique article ID from URL."""
    return hashlib.md5(link.strip().encode("utf-8")).hexdigest()


def fetch_single_feed(source_name: str, rss_url: str) -> List[Dict[str, Any]]:
    """Fetch and parse a single RSS feed."""
    try:
        logger.info("Fetching RSS feed from %s: %s", source_name, rss_url)
        response = requests.get(rss_url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        feed = feedparser.parse(response.content)

        if getattr(feed, "bozo", 0):
            logger.warning(
                "Parsing issue in %s: %s",
                source_name,
                getattr(feed, "bozo_exception", "Unknown error"),
            )

        records: List[Dict[str, Any]] = []

        for entry in feed.entries:
            link = entry.get("link", "").strip()
            title = entry.get("title", "").strip()

            if not link or not title:
                logger.debug("Skipping entry with missing link or title")
                continue

            record = {
                "article_id": generate_article_id(link),
                "source": source_name,
                "title": title,
                "link": link,
                "published_at": entry.get("published", "") or entry.get("updated", ""),
                "summary": entry.get("summary", "").strip(),
                "loaded_at": datetime.now(timezone.utc),
            }
            records.append(record)

        logger.info("Fetched %d records from %s", len(records), source_name)
        return records

    except requests.RequestException as e:
        logger.error("HTTP error fetching %s: %s", source_name, e)
        return []
    except Exception as e:
        logger.error("Unexpected error fetching %s: %s", source_name, e)
        return []


def fetch_all_feeds() -> List[Dict[str, Any]]:
    """Fetch and parse all configured RSS feeds."""
    all_records: List[Dict[str, Any]] = []

    for source in RSS_SOURCES:
        records = fetch_single_feed(source["name"], source["url"])
        all_records.extend(records)

    logger.info("Total records fetched from all sources: %d", len(all_records))
    return all_records


if __name__ == "__main__":
    data = fetch_all_feeds()
    logger.info("Total records fetched: %d", len(data))
    for row in data[:3]:
        logger.info("Sample: %s", row)