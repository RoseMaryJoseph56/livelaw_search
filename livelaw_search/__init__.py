from flask import Flask, redirect, url_for


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__)

    app.config.from_pyfile('config.py')
    
    from . import search
    app.register_blueprint(search.bp)


    @app.route("/")
    def hello():
        return redirect(url_for("livelaw.search"))
        
    return app


   