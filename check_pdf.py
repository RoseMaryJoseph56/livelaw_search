import os
import re
import unicodedata
import pdfplumber
import requests
from logging import exception
from operator import index
import mysql.connector
from elasticsearch import Elasticsearch, helpers
from bs4 import BeautifulSoup

es = Elasticsearch("http://52.7.83.219:9200")

db = mysql.connector.connect(
    user="root",
    password="password",
    host="localhost",
    database="livelaw_dump",
    auth_plugin="mysql_native_password",
)
BASE_DIR = os.getcwd()


def fetch_data_from_sql():
    """Function to fetch a set of data from db and insert to index"""
    count_query = "SELECT COUNT(*) FROM rdes_news"
    cursor = db.cursor()
    cursor.execute(count_query)
    count = cursor.fetchall()
    select_query = "select * from rdes_news"
    cursor.execute(select_query)
    for i in range(0, count[0][0], 100):
        data = cursor.fetchmany(100)
        insert_to_index(i, es, data)


def create_index():
    """Function to create index with specific analyzer and mapping"""
    es.indices.create(
        index="livelaw_search",
        body={
            "settings": {
                "analysis": {
                    "analyzer": {
                        "my_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "char_filter": ["html_strip"],
                            "filter": ["lowercase"],
                        }
                    },
                }
            },
            "mappings": {
                "properties": {
                    "heading": {"type": "text", "analyzer": "my_analyzer"},
                    "id": {"type": "keyword"},
                    "date": {"type": "date"},
                    "keywords": {"type": "text", "analyzer": "my_analyzer"},
                    "content": {"type": "text", "analyzer": "my_analyzer"},
                    "pdf_content": {"type": "text", "analyzer": "my_analyzer"},
                }
            },
        },
        ignore=400,
    )


def get_pdf_response(url):
    """get pdf file path and write pdf content to the file"""
    pdf_file_path = BASE_DIR + "/pdf_file.pdf"
    get_pdf_response = requests.get(url)
    pdf_data = open(pdf_file_path, "wb")
    pdf_data.write(get_pdf_response.content)
    return pdf_file_path


def extract_text_from_pdf(pdf_file_path):
    """extracting pdf text using pdf plumber and modifying to proper format"""
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
                curr_pg = re.sub("\d+\s+", "", curr_pg)
                curr_pg = re.sub("Page", "", curr_pg)
                pdf_content = pdf_content + "\n" + curr_pg
    except Exception as e:
        print(f"caught exception : {str(e)}")
    return pdf_content


def insert_to_index(data_count, es, data):
    """Function to check url present in content
    and calls functions to extract url pdf and insert all datas to index
    """
    json_data = []
    for result in data:
        parser = BeautifulSoup(result[3], "html.parser")
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
                "id": result[0],
                "heading": result[1],
                "content": result[3],
                "date": result[11],
                "keywords": result[38],
                "pdf_content": pdf_content,
            }
        else:
            articles = {
                "id": result[0],
                "heading": result[1],
                "content": result[3],
                "date": result[11],
                "keywords": result[38],
                "pdf_content": None,
            }
        json_data.append(articles)
    print(f"inserting {data_count} data")
    try:
        res = helpers.bulk(es, json_data, index="livelaw_search")
    except Exception as e:
        es = Elasticsearch("http://52.7.83.219:9200")
        print(f"caught exception {str(e)}")
        insert_to_index(data_count, es, data)


if not es.indices.exists(index="livelaw_search"):
    create_index()
fetch_data_from_sql()
