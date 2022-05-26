import os
import pdfplumber
import re
import requests
import unicodedata
from PyPDF2 import PdfFileReader
from celery import Celery
from flask import Blueprint
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup


dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

ES_HOST = os.environ.get("ES_HOST")
es = Elasticsearch(ES_HOST)

bp = Blueprint("task", __name__, url_prefix="/news")
app1 = Celery("task", broker="redis://localhost:6379", backend="redis://localhost:6379")

BASE_DIR = os.getcwd()

def extract_text_from_pdf(pdf_file_path):
    print(pdf_file_path)
    with pdfplumber.open(pdf_file_path) as pdf:
        for page in pdf.pages:
            curr_pg=page.extract_text()
            normalized_data = unicodedata.normalize("NFKD", curr_pg)
            pdf_data = re.sub('Page (\d+)', '', normalized_data)
            curr_pg=pdf_data.strip()
            curr_pg=re.sub('\s+', ' ', curr_pg)
    return curr_pg

def get_pdf_response(url):
    """get pdf"""
    pdf_file_path = BASE_DIR + "temp_news.pdf"
    get_pdf_response = requests.get(url)
    pdf_data = open(pdf_file_path, "wb")
    pdf_data.write(get_pdf_response.content)
    return pdf_file_path
    

@app1.task()
def insert_to_index(news_data):
    """Function to insert data to index"""
    for data in news_data:
        if set(("id", "content", "keywords", "heading", "date")).issubset(data.keys()):
            content = data.get("content")
            html_content = content.replace('"', '\\"')
            parser = BeautifulSoup(html_content, "html.parser")
            if parser.embed:
                parser_url = parser.embed
                url = parser_url.attrs.get("src", "").split("&url=")[-1]
                pdf_file_path = get_pdf_response(url)
                pdf_content = extract_text_from_pdf(pdf_file_path)
                articles = {
                    "id": data.get("id"),
                    "heading": data.get("heading"),
                    "content": data.get("content"),
                    "date": data.get("date"),
                    "keywords": data.get("keywords"),
                    "pdf_content": pdf_content,
                }
            else:
                articles = {
                    "id": data.get("id"),
                    "heading": data.get("heading"),
                    "content": data.get("content"),
                    "date": data.get("date"),
                    "keywords": data.get("keywords"),
                }
            res = es.index(index="livelaw", body=articles)
