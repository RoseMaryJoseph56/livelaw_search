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
from .config import DATABASE, ENDPOINT, ES_HOST, HEADER, HOST, INDEX, PASSWORD, REDIS, MYSQL_USER, SID
import mysql.connector

es = Elasticsearch(ES_HOST)
bp = Blueprint("task", __name__, url_prefix="/news")
app1 = Celery("task", broker=REDIS, backend=REDIS)

BASE_DIR = os.getcwd()
parsed_date = str(datetime.now())

db = mysql.connector.connect(
    user=MYSQL_USER,
    password=PASSWORD,
    host=HOST,
    database=DATABASE,
    auth_plugin="mysql_native_password",
)


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
    insert_to_table_query = """ insert into es_status (
                                news_id, status, error_message,
                                date_news, parsed_date) select
                                %(news_id)s,%(status)s,%(error_message)s,
                                %(date_news)s,%(parsed_date)s
                                where not exists (select news_id from es_status 
                                where news_id = %(news_id)s and status = %(status)s
                                )
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


def fetch_failed_data_from_sql():
    """Function to fetch failed data id from sql table"""
    count_query = "SELECT COUNT(*) FROM es_status where status = 'fail'"
    failed_data_query = "select news_id from es_status where status = 'fail'"
    cur = db.cursor()
    cur.execute(count_query)
    count = cur.fetchall()
    cursor = db.cursor(dictionary=True)
    cursor.execute(failed_data_query)
    failed_data_count = count[0][0]
    failed_data = cursor.fetchall()
    return failed_data, failed_data_count

def get_data_from_api(failed_data):
    failed_data_list = []
    for data in failed_data:
        id = data.get("news_id")
        response = requests.post(
            ENDPOINT,
            headers={
                "Content-Type": HEADER,
                "s-id": SID,
            },
            data={"newsId": id},
        )
        json_data = response.json()
        json_list = json_data.get("news")
        data = json_list[0] if len(json_list)>0 else {}

        articles = {
            "id": data.get("newsId"),
            "heading": data.get("heading"),
            "content": data.get("story"),
            "keywords": data.get("keywords"),
            "date": data.get("date_news"),
        }
        if articles["date"]:
            failed_data_list.append(articles)
    return failed_data_list


def extract_pdf_or_insert_to_index(
    pdf_file_path,
    id,
    heading,
    content,
    keywords,
    date,
):
    """extracting failed pdf or insert data to index"""
    sql_insert_list = []
    json_data = []
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
        articles = {
            "id": id,
            "heading": heading,
            "content": content,
            "date": date,
            "keywords": keywords,
            "pdf_content": None,
        }
        insert_data = {
            "news_id": id,
            "status": "success",
            "error_message": "",
            "date_news": date,
            "parsed_date": parsed_date,
        }
        sql_insert_list.append(insert_data)
        json_data.append(articles)
        mysql_status_table(sql_insert_list)
        res = helpers.bulk(es, json_data, index=INDEX)
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
            if insert_data not in sql_insert_list:
                sql_insert_list.append(insert_data)
            if articles not in json_data:
                json_data.append(articles)
    mysql_status_table(sql_insert_list)
    res = helpers.bulk(es, json_data, index=INDEX)


@app1.task()
def insert_failed_data_to_index():
    """Function to insert data with pdf parsing error to index"""
    sql_insert_list = []
    json_data = []
    failed_data, failed_data_count = fetch_failed_data_from_sql()
    failed_data_list = get_data_from_api(failed_data)
    for data in failed_data_list:
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
                pdf_content = extract_pdf_or_insert_to_index(
                    pdf_file_path,
                    data.get("id"),
                    data.get("heading"),
                    data.get("content"),
                    data.get("keywords"),
                    data.get("date"),
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
                }
            insert_data = {
                "news_id": data.get("id"),
                "status": "success",
                "error_message": "",
                "date_news": data.get("date"),
                "parsed_date": parsed_date,
            }
            if insert_data not in sql_insert_list:
                sql_insert_list.append(insert_data)
            if articles not in json_data:
                json_data.append(articles)
                print(articles["date"])
    res = helpers.bulk(es, json_data, index=INDEX)
    mysql_status_table(sql_insert_list)
    return failed_data_count
