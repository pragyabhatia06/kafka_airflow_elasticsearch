# Real-Time RSS News Data Pipeline

This project ingests RSS news feeds, publishes articles to Kafka, stores them in MongoDB, indexes them into Elasticsearch, and exposes the stored news through a Flask API. Apache Airflow orchestrates the ingestion pipeline on a schedule.

## Overview

The pipeline fetches RSS feeds from multiple sources, normalizes the articles, and sends them through Kafka so that multiple downstream consumers can process the same stream independently.

Current sources:

- BBC News RSS
- Sky News RSS

Current outputs:

- MongoDB for durable document storage and API reads
- Elasticsearch for search-oriented indexing

## Architecture

```text
RSS Feeds
   |
   v
scraper/rss_fetcher.py
   |
   v
scraper/producer.py
   |
   v
Kafka topic: rss_news_raw
   |                     \
   |                      \
   v                       v
consumers/mongo_consumer.py  consumers/elastic_consumer.py
   |                       |
   v                       v
MongoDB                 Elasticsearch
   |
   v
Flask API

Airflow scheduler triggers the producer every 10 minutes.
```

## Key Features

- Scheduled RSS ingestion with Airflow
- Kafka-based decoupled processing
- MongoDB upsert pipeline with unique article IDs
- Elasticsearch indexing for search use cases
- Flask API for reading ingested articles
- Docker Compose based local development setup
- Retry logic for Kafka, MongoDB, and Elasticsearch connectivity

## Tech Stack

- Python 3.11
- Flask
- Apache Airflow 2.10
- Kafka + Zookeeper
- MongoDB
- Elasticsearch 8.15
- Docker Compose

## Project Structure

```text
.
├── airflow/
│   └── dags/
│       └── rss_pipeline_dag.py
├── api/
│   ├── __init__.py
│   ├── app.py
│   └── rss.py
├── consumers/
│   ├── __init__.py
│   ├── elastic_consumer.py
│   └── mongo_consumer.py
├── scraper/
│   ├── __init__.py
│   ├── load_to_mongo.py
│   ├── producer.py
│   └── rss_fetcher.py
├── docker-compose.yml
├── dockerfile
├── requirements.txt
└── .env
```

## How the Pipeline Works

### 1. Fetch

The RSS fetcher retrieves articles from configured feed URLs and builds normalized records with these fields:

- article_id
- source
- title
- link
- published_at
- summary
- loaded_at

`article_id` is generated from the article URL using an MD5 hash, which allows idempotent upserts in downstream systems.

### 2. Publish

The producer publishes each article to Kafka topic `rss_news_raw`.

Producer behavior:

- retries Kafka connection on startup
- sends JSON-serialized article payloads
- waits for broker acknowledgment
- logs per-run success and failure counts

### 3. Consume to MongoDB

The MongoDB consumer reads from Kafka and upserts documents into MongoDB collection `raw_articles`.

Mongo consumer behavior:

- creates useful indexes
- upserts by `article_id`
- avoids duplicate records across re-runs
- logs insert/modify/match counts

### 4. Consume to Elasticsearch

The Elasticsearch consumer reads the same Kafka topic and indexes the records into Elasticsearch index `news_articles`.

Elasticsearch consumer behavior:

- waits for Elasticsearch readiness
- creates index mappings if missing
- normalizes date values before indexing
- logs successful indexing events

### 5. Serve via API

The Flask API reads data from MongoDB and exposes read endpoints for health checks and article retrieval.

## Services

The Docker Compose stack includes the following services:

| Service | Purpose | Port |
|---|---|---|
| `mongo` | Stores ingested articles | `27017` |
| `api` | Flask API server | `5000` |
| `airflow-db` | Airflow metadata database | `5433` |
| `airflow-init` | Initializes Airflow DB and admin user | none |
| `airflow-webserver` | Airflow UI | `8080` |
| `airflow-scheduler` | Runs DAG schedule | none |
| `zookeeper` | Kafka coordination | none |
| `kafka` | Message broker | `9092` |
| `mongo-consumer` | Persists Kafka messages into MongoDB | none |
| `elasticsearch` | Search index engine | `9200` |
| `elastic-consumer` | Persists Kafka messages into Elasticsearch | none |

## Environment Variables

The project uses the following environment variables from `.env`:

```env
MONGO_URI=mongodb://mongo:27017/
MONGO_DB=news_db
MONGO_COLLECTION=raw_articles

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=rss_news_raw
KAFKA_CONSUMER_GROUP=mongo-news-consumer-group

ELASTICSEARCH_HOST=http://elasticsearch:9200
ELASTICSEARCH_INDEX=news_articles
ELASTIC_CONSUMER_GROUP=elastic-news-consumer-group
```

## Running the Project

### Prerequisites

- Docker Desktop or Docker Engine with Compose support
- At least 4 GB of memory available for local containers

### Start the Stack

From the project root:

```bash
docker compose up --build
```

To run in detached mode:

```bash
docker compose up -d --build
```

### Airflow Access

Open Airflow at:

```text
http://localhost:8080
```

Default admin credentials configured by the stack:

- Username: `admin`
- Password: `admin`

### API Access

Open the API at:

```text
http://localhost:5000
```

### Elasticsearch Access

Open Elasticsearch at:

```text
http://localhost:9200
```

## Airflow DAG

The main DAG is:

- DAG ID: `rss_to_kafka_pipeline`

Schedule:

- Every 10 minutes

Task:

- `publish_rss_news`

Notes:

- Airflow may show the DAG as paused on first startup depending on metadata state.
- If the DAG is paused, unpause it before expecting scheduled ingestion.

Useful commands:

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags unpause rss_to_kafka_pipeline
docker compose exec airflow-webserver airflow dags trigger rss_to_kafka_pipeline
```

## API Endpoints

### Health Check

```http
GET /health
```

Example:

```bash
curl http://localhost:5000/health
```

Response:

```json
{
  "status": "ok"
}
```

### Get Latest News

```http
GET /news
```

Optional query parameters:

- `source`: filter by source name
- `limit`: number of articles to return, between 1 and 100

Examples:

```bash
curl "http://localhost:5000/news"
curl "http://localhost:5000/news?limit=5"
curl "http://localhost:5000/news?source=BBC&limit=10"
```

### Get Article by ID

```http
GET /news/<article_id>
```

Example:

```bash
curl "http://localhost:5000/news/be49b0ce372a3ae8a89cc64c3ea60b1f"
```

## Verifying the Pipeline

### Check Running Services

```bash
docker compose ps
```

### Check MongoDB Document Count

```bash
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('news_db').raw_articles.countDocuments()"
```

### Check Elasticsearch Document Count

```bash
docker compose exec elasticsearch curl -s http://localhost:9200/news_articles/_count
```

### Inspect Kafka/Mongo/Elasticsearch Logs

```bash
docker compose logs --tail=100 kafka
docker compose logs --tail=100 mongo-consumer
docker compose logs --tail=100 elastic-consumer
docker compose logs --tail=100 airflow-scheduler
```

## Sample End-to-End Workflow

1. Start the stack.
2. Open Airflow UI at `http://localhost:8080`.
3. Confirm `rss_to_kafka_pipeline` exists.
4. Unpause the DAG if needed.
5. Trigger a manual DAG run for immediate ingestion.
6. Confirm records appear in MongoDB.
7. Confirm records appear in Elasticsearch.
8. Call the Flask API to retrieve articles.

## Data Model

Each article record follows this shape:

```json
{
  "article_id": "string",
  "source": "BBC",
  "title": "Article title",
  "link": "https://...",
  "published_at": "Thu, 12 Mar 2026 15:09:00 +0000",
  "summary": "Short summary",
  "loaded_at": "2026-03-12T19:20:12.498498+00:00"
}
```

## Local Development Notes

### Docker Image

The project image is built from `python:3.11-slim` and installs dependencies from `requirements.txt`.

Default container command:

```text
gunicorn --bind 0.0.0.0:5000 api.app:app
```

### Python Dependencies

Main dependencies:

- `flask`
- `pymongo`
- `feedparser`
- `requests`
- `python-dotenv`
- `gunicorn`
- `kafka-python`
- `elasticsearch==8.15.1`

## Troubleshooting

### No News Appears in MongoDB or API

Check the following:

1. The stack is running:

```bash
docker compose ps
```

2. The Airflow DAG is not paused:

```bash
docker compose exec airflow-webserver airflow dags list
```

3. Trigger the DAG manually:

```bash
docker compose exec airflow-webserver airflow dags trigger rss_to_kafka_pipeline
```

4. Inspect producer and consumer logs:

```bash
docker compose logs --tail=100 airflow-scheduler
docker compose logs --tail=100 mongo-consumer
```

### Kafka Consumer Cannot Connect

Check:

- `kafka` service is up
- `zookeeper` service is up
- broker is listening on `kafka:9092`

Useful command:

```bash
docker compose logs --tail=100 kafka
```

### Elasticsearch Index Is Empty

Check:

1. Elasticsearch is running:

```bash
docker compose ps elasticsearch elastic-consumer
```

2. The consumer is healthy:

```bash
docker compose logs --tail=100 elastic-consumer
```

3. The index count:

```bash
docker compose exec elasticsearch curl -s http://localhost:9200/news_articles/_count
```

### Airflow Starts but Pipeline Does Not Run Automatically

Possible causes:

- DAG paused in metadata
- scheduler restarted before DAG state settled
- ingestion window has not reached the next 10-minute interval

Use a manual trigger to verify the pipeline immediately.

## Deprecated Component

`scraper/load_to_mongo.py` is kept only as a backward-compatible direct insert path. The primary pipeline is Kafka-based and should be preferred.

## Current Status

The project currently supports:

- scheduled feed ingestion
- MongoDB persistence
- Elasticsearch indexing
- API retrieval from MongoDB
- local orchestration with Docker Compose

## Possible Next Enhancements

- Add full-text search API backed by Elasticsearch
- Add Swagger or OpenAPI documentation
- Add unit and integration tests
- Add health checks in Docker Compose
- Build a custom Airflow image instead of installing packages at startup
- Add dashboards and monitoring
