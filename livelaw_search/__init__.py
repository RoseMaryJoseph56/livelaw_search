import os
from dotenv import load_dotenv
from flask import Flask
from elasticsearch import Elasticsearch

load_dotenv('.env')
def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__)
    
    from . import search
    app.register_blueprint(search.bp)
    
    # a simple page that says hello
    @app.route("/hello")
    def hello():
        return "Hello, World!"

    return app