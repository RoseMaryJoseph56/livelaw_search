from datetime import datetime
import os
import pdfplumber
import re
import requests
import unicodedata
from celery import Celery
from flask import Blueprint, Flask
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from bs4 import BeautifulSoup
from .config import DATABASE, ES_HOST, HOST, INDEX, PASSWORD, REDIS, MYSQL_USER
import mysql.connector


es = Elasticsearch(ES_HOST)
bp = Blueprint("task", __name__, url_prefix="/news")
app1 = Celery("task", broker=REDIS, backend=REDIS)

BASE_DIR = os.getcwd()
parsed_date = str(datetime.now())


def mysql_status_table(sql_insert_list):
    """function to create mysql table to track news
    insertion status on elasticsearch
    """
    db = mysql.connector.connect(
        user=MYSQL_USER,
        password=PASSWORD,
        host=HOST,
        database=DATABASE,
        auth_plugin="mysql_native_password",
    )
    create_table_query = """create table if not exists es_status (
                                news_id int not null ,
                                status  varchar(200),
                                error_message varchar(200),
                                date_news varchar(200),
                                parsed_date datetime
                                )"""
    insert_to_table_query = """insert into es_status (
                                news_id, status, error_message,
                                date_news, parsed_date) values (
                                %(news_id)s,%(status)s,%(error_message)s,
                                %(date_news)s,%(parsed_date)s)
                                """
    cursor = db.cursor()
    cursor.execute(create_table_query)
    cursor.executemany(insert_to_table_query, sql_insert_list)
    db.commit()


def extract_text_from_pdf(pdf_file_path, id, date):
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
                pdf_content = pdf_content + curr_pg
    except Exception as e:
        insert_data = {
            "news_id": id,
            "status": "fail",
            "error_message": "pdf parsing error",
            "date_news": date,
            "parsed_date": parsed_date,
        }
        sql_insert_list.append(insert_data)
        mysql_status_table(sql_insert_list)
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
    sql_insert_list = []
    json_data = []
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
                pdf_file_path = get_pdf_response(url)
                pdf_content = extract_text_from_pdf(
                    pdf_file_path, data.get("id"), data.get("date")
                )
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
                    "pdf_content": None,
                }
            insert_data = {
                "news_id": data.get("id"),
                "status": "success",
                "error_message": "",
                "date_news": data.get("date"),
                "parsed_date": parsed_date,
            }
            sql_insert_list.append(insert_data)
            json_data.append(articles)
            res = helpers.bulk(es, json_data, index=INDEX)
            mysql_status_table(sql_insert_list)
