from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from scrapers.foxnews_scraper import scrape_foxnews
from scrapers.cnn_scraper import scrape_cnn
from scrapers.apnews_scraper import scrape_apnews
from scrapers.meduza_scraper import scrape_meduza
from scrapers.pbsnewshour_scraper import scrape_pbsnewshour
from scrapers.washingtonpost_scraper import scrape_washingtonpost
from scrapers.washingtontimes_scraper import scrape_washingtontimes

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('news_scraper', default_args=default_args, schedule_interval='@daily') as dag:

    scrape_foxnews_task = PythonOperator(
        task_id='scrape_foxnews',
        python_callable=scrape_foxnews
    )

    scrape_cnn_task = PythonOperator(
        task_id='scrape_cnn',
        python_callable=scrape_cnn
    )

    scrape_apnews_task = PythonOperator(
        task_id='scrape_apnews',
        python_callable=scrape_apnews
    )

    scrape_meduza_task = PythonOperator(
        task_id='scrape_meduza',
        python_callable=scrape_meduza
    )

    scrape_pbsnewshour_task = PythonOperator(
        task_id='scrape_pbsnewshour',
        python_callable=scrape_pbsnewshour
    )

    scrape_washingtonpost_task = PythonOperator(
        task_id='scrape_washingtonpost',
        python_callable=scrape_washingtonpost
    )

    scrape_washingtontimes_task = PythonOperator(
        task_id='scrape_washingtontimes',
        python_callable=scrape_washingtontimes
    )

    (
        scrape_foxnews_task,
        scrape_cnn_task,
        scrape_apnews_task,
        scrape_meduza_task,
        scrape_pbsnewshour_task,
        scrape_washingtonpost_task,
        scrape_washingtontimes_task
    )
    