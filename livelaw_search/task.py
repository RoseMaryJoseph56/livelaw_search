import os
import pdfplumber
import re
import requests
import unicodedata
from celery import Celery
from flask import Blueprint
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from .config import ES_HOST, REDIS

es = Elasticsearch(ES_HOST)
bp = Blueprint("task", __name__, url_prefix="/news")
app1 = Celery("task", broker=REDIS, backend=REDIS)

BASE_DIR = os.getcwd()


def extract_text_from_pdf(pdf_file_path):
    try:
        with pdfplumber.open(pdf_file_path) as pdf:
            pdf_content = ""
            for page in pdf.pages:
                curr_pg = page.extract_text()
                normalized_data = unicodedata.normalize("NFKD", curr_pg)
                pdf_data = re.sub("Page (\d+)", "", normalized_data)
                curr_pg = re.sub("\d+\s+of\s+\d+", "", curr_pg)
                curr_pg = pdf_data.strip()
                curr_pg = re.sub("(-\s+\d+\s+-)|(-\d+-)|(...\d+/-)", "", curr_pg)
                curr_pg = re.sub("\d+$", "", curr_pg)
                curr_pg = re.sub("\d+\s+", "", curr_pg)
                curr_pg = re.sub("Page", "", curr_pg)
                pdf_content = pdf_content + curr_pg
    except Exception as e:
        print(f"caught exception : {str(e)}")
    return pdf_content


def get_pdf_response(url):
    """get pdf file path and write pdf content to the file"""
    pdf_file_path = BASE_DIR + "/temp_news.pdf"
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
            parser = BeautifulSoup(content, "html.parser")
            if parser.embed:
                parser_url = parser.embed
                url = parser_url.attrs.get("src", "").split("&url=")[-1]
                if url.endswith('\\"'):
                    url = url[:-2]
                if url.startswith("www"):
                    url = "http://" + url
                print(url)
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
