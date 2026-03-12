from datetime import datetime, timedelta
import sys

sys.path.append("/opt/project")

from airflow import DAG
from airflow.operators.python import PythonOperator
from scraper.producer import publish_articles

default_args = {
    "owner": "pragya",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_rss_pipeline():
    publish_articles()


with DAG(
    dag_id="rss_to_kafka_pipeline",
    default_args=default_args,
    description="Fetch RSS feeds and publish them to Kafka",
    start_date=datetime(2026, 3, 10),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["rss", "kafka", "news"],
) as dag:

    publish_rss_news = PythonOperator(
        task_id="publish_rss_news",
        python_callable=run_rss_pipeline,
    )

    publish_rss_news