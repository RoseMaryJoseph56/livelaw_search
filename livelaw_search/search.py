import os
from flask import (
    render_template,
    request,
    Blueprint,
)
from elasticsearch import Elasticsearch

ES_HOST = os.environ.get("ES_HOST")
es = Elasticsearch("http://localhost:9200")

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
