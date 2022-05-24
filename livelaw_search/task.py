import os
from celery import Celery
from flask import Blueprint
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

ES_HOST = os.environ.get("ES_HOST")
es = Elasticsearch(ES_HOST)

bp = Blueprint("task", __name__, url_prefix="/news")
app1 = Celery("task", broker="redis://localhost:6379", backend="redis://localhost:6379")


@app1.task()
def insert_to_index(news_data):
    """Function to insert data to index"""
    for data in news_data:
        if set(("id", "content", "keywords", "heading", "date")).issubset(data.keys()):
            res = es.index(index="livelaw", body=data)
