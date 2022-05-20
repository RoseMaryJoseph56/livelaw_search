import os
from flask_restful import Resource
from flask import (
    render_template,
    request,
    Blueprint,
)
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from flasgger import SwaggerView
from .task import insert_to_index
from .api_docs import search_api_parameters, search_api_responses,insert_api_parameters,insert_api_responses

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

ES_HOST = os.environ.get("ES_HOST")
es = Elasticsearch(ES_HOST)
bp = Blueprint("livelaw", __name__, url_prefix="/news")


@bp.route("/search/", methods=("GET", "POST"))
def search():
    """Function to search and load data with pagination"""
    from_value = 0
    if request.method == "POST":
        search = request.form["Search"]
        current_page = request.form["page"]
        from_value = int(current_page) * 10
        body = {
            "query": {
                "multi_match": {
                    "query": search,
                    "fields": ["content", "heading", "keywords"],
                    "operator": "and",
                    "type": "phrase",
                }
            },
            "_source": ["heading", "id", "date"],
            "from": from_value,
            "size": 20,
            "sort": [{"date": "desc"}],
            "track_total_hits": True,
        }
        page = es.search(index="livelaw", body=body)
        search_result = page["hits"]["hits"]
        search_count = page["hits"]["total"]["value"]

        return render_template(
            "search.html",
            search_result=search_result,
            search=search,
            page=int(current_page) + 1,
            search_count=search_count,
        )
    return render_template("search.html")


def get_data(search_term, current_page=0):
    """Function to search and returns data and count"""
    from_value = current_page * 20
    body = {
        "query": {
            "multi_match": {
                "query": search_term,
                "fields": ["content", "heading", "keywords"],
                "operator": "and",
                "type": "phrase",
            }
        },
        "_source": ["heading", "id"],
        "from": from_value,
        "size": 20,
        "sort": [{"date": "desc"}],
        "track_total_hits": True,
    }
    page = es.search(index="livelaw", body=body)
    search_result = page["hits"]["hits"]
    news_result = []
    for i in search_result:
        news_result.append(i["_source"])
    search_count = page["hits"]["total"]["value"]
    return search_count, news_result, current_page + 1


class SearchNewsArticleApi(Resource, SwaggerView):
    parameters = search_api_parameters
    responses = search_api_responses

    def post(self):
        """Returns news articles related to search query"""
        data = request.get_json(force=True)
        page_number = data.get("page", 0)
        search_query = data.get("search_term", "")

        if search_query:
            total_count, search_result, current_page = get_data(
                search_query, current_page=page_number
            )
            result_data = {
                "total_articles": total_count,
                "search_result": search_result,
                "current_page": current_page,
            }
            return result_data, 200

        data = {"message": "Please enter search query"}
        return result_data, 400


class InsertNewsArticlesApi(Resource, SwaggerView):
    parameters=insert_api_parameters
    responses=insert_api_responses

    def post(self):
        """Function to get request data and call insert function"""
        news_data = request.get_json(force=True)
        insert_to_index(news_data)
        return {"message": "insertion task initiated"}, 201