from flask import Flask, redirect, url_for
from flask_restful import Resource, Api

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__)
    api = Api(app)

    app.config.from_pyfile('config.py')
    
    from . import search
    app.register_blueprint(search.bp)


    @app.route("/")
    def main():
        return redirect(url_for("livelaw.search"))

    api.add_resource(search.SearchNewsArticleApi, '/search')
    api.add_resource(search.InsertNewsArticlesApi, '/insert')
    return app
    
    


   