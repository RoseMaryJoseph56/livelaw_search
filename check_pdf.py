from datetime import datetime
import os
import re
import unicodedata
import pdfplumber
import requests
from logging import exception
from operator import index
import mysql.connector
import sys
from elasticsearch import Elasticsearch, helpers
from bs4 import BeautifulSoup

es = Elasticsearch("http://localhost:9200")

db = mysql.connector.connect(
    user="root",
    password="password",
    host="localhost",
    database="livelaw_status",
    auth_plugin="mysql_native_password",
)

BASE_DIR = os.getcwd()
parsed_date = str(datetime.now())


def fetch_data_from_sql():
    """Function to fetch a set of data from db and insert to index"""
    count_query = "SELECT COUNT(*) FROM rdes_news"
    cursor = db.cursor(dictionary=True)
    cursor.execute(count_query)
    count = cursor.fetchall()
    select_query = "select * from rdes_news where date_news > '2022-02-20 00:00:00'"
    cursor.execute(select_query)
    for i in range(0, count[0]["COUNT(*)"], 100):
        data = cursor.fetchmany(100)
        insert_to_index(i, es, data)


def mysql_status_table(sql_insert_list):
    """function to create mysql table to track news
    insertion status on elasticsearch
    """
    create_table_query = """create table if not exists es_status (
                                news_id int not null ,
                                status  varchar(200),
                                error_message varchar(200),
                                date_news varchar(200),
                                parsed_date datetime
                                )"""
    insert_to_table_query = """insert into es_status (
                                news_id, status, error_message,
                                date_news, parsed_date) values (%(news_id)s,
                                %(status)s,%(error_message)s,%(date_news)s,%(parsed_date)s)"""
    cursor = db.cursor()
    cursor.execute(create_table_query)
    cursor.executemany(insert_to_table_query, sql_insert_list)
    db.commit()


def create_index():
    """Function to create index with specific analyzer and mapping"""
    es.indices.create(
        index="livelaw",
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


def extract_text_from_pdf(pdf_file_path, uid, date_news):
    """extracting pdf text using pdf plumber and modifying to proper format"""
    sql_insert_list = []
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
                curr_pg = re.sub("Page", "", curr_pg)
                pdf_content = pdf_content + "\n" + curr_pg
    except Exception as e:
        insert_data = {
            "news_id": uid,
            "status": "fail",
            "error_message": "pdf parsing error",
            "date_news": date_news,
            "parsed_date": parsed_date,
        }
        sql_insert_list.append(insert_data)
        mysql_status_table(sql_insert_list)
        print(f"caught exception {str(e)}")
    return pdf_content


def insert_to_index(data_count, es, data):
    """Function to check url present in content
    and calls functions to extract url pdf and insert all datas to index
    """
    json_data = []
    sql_insert_list = []
    for result in data:
        parser = BeautifulSoup(result["story"], "html.parser")
        if parser.embed:
            parser_url = parser.embed
            url = parser_url.attrs.get("src", "").split("&url=")[-1]
            if url.endswith('\\"'):
                url = url[:-2]
            if url.startswith("www"):
                url = "http://" + url
            print(url)
            pdf_file_path = get_pdf_response(url)
            pdf_content = extract_text_from_pdf(
                pdf_file_path, result["uid"], result["date_news"]
            )
            articles = {
                "id": result["uid"],
                "heading": result["heading"],
                "content": result["story"],
                "date": result["date_news"],
                "keywords": result["keywords"],
                "pdf_content": pdf_content,
            }
        else:
            articles = {
                "id": result["uid"],
                "heading": result["heading"],
                "content": result["story"],
                "date": result["date_news"],
                "keywords": result["keywords"],
            }
        insert_data = {
            "news_id": result["uid"],
            "status": "success",
            "error_message": "",
            "date_news": result["date_news"],
            "parsed_date": parsed_date,
        }
        json_data.append(articles)
        sql_insert_list.append(insert_data)
    print(f"inserting {data_count} data")
    try:
        mysql_status_table(sql_insert_list)
        res = helpers.bulk(es, json_data, index="livelaw")
    except Exception as e:
        es = Elasticsearch("http://localhost:9200")
        print(f"caught exception {str(e)}")
        insert_to_index(data_count, es, data)


if not es.indices.exists(index="livelaw"):
    create_index()
fetch_data_from_sql()
