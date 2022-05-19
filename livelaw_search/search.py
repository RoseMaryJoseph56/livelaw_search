import os
from flask_restful import Resource
from flask import (
    render_template,
    request,
    Blueprint,
)
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
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
            "track_total_hits": True
        }
        page = es.search(index="livelaw", body=body)
        search_result = page["hits"]["hits"]
        search_count = page["hits"]["total"]["value"]

        return render_template(
            "search.html", search_result=search_result, search=search,
            page=int(current_page) + 1, search_count=search_count
        )
    return render_template("search.html")

def get_data(search_term , current_page=0):
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
            "track_total_hits": True
        }
    page = es.search(index="livelaw", body=body)
    search_result = page["hits"]["hits"]
    search_count = page["hits"]["total"]["value"]
    return search_count, search_result, current_page + 1


class SearchNewsArticleApi(Resource):
    
    def post(self):
        """Function to get json input and retuns related data"""
        data = request.get_json(force=True)
        page_number = data.get("page", 0)
        search_query = data.get("search_term", "")
        total_count, search_result, current_page = get_data(search_query, current_page = page_number)
        result_data = {
            "total_articles": total_count, "search_result": search_result, "current_page": current_page
            }
        return result_data, 201
        

class InsertNewsArticlesApi(Resource):

    def post(self):
        """Function to insert input news data to index"""
        news_data = request.get_json(force=True)
        if set(("id", "content", "keywords", "heading", "date")).issubset(news_data.keys()):
            res = es.index(index="livelaw", body=news_data)
        
            return {"message": "success"}, 201
        
        return {"message": "please check input data"}, 400