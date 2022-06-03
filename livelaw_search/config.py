import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

ES_HOST = os.environ.get("ES_HOST")
MYSQL_USER = os.environ.get("MYSQL_USER")
PASSWORD = os.environ.get("PASSWORD")
HOST = os.environ.get("HOST")
DATABASE = os.environ.get("DATABASE")
REDIS = os.environ.get("REDIS")
INDEX = os.environ.get("INDEX")