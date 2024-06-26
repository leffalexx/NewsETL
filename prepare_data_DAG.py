from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import mysql.connector
from bias_analysis import predict_political_bias

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

db_config = {
    'host': 'localhost',
    'user': 'alexx',
    'password': '1606',
    'database': 'article_scraper'
}


def execute_sql(sql, params=None):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    try:
        cursor.execute(sql, params)
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def clean_data(**context):
    execution_date = context['execution_date']
    sql = """
    DELETE FROM articles
    WHERE (title = '' OR content = '' OR LENGTH(url) < 40)
    AND created_at >= %s AND created_at < %s;
    """
    params = (execution_date - timedelta(days=1), execution_date)
    execute_sql(sql, params)


def analyze_bias(**context):
    execution_date = context['execution_date']
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT id, content FROM articles
            WHERE created_at >= %s AND created_at < %s
        """, (execution_date - timedelta(days=1), execution_date))
        articles = cursor.fetchall()

        for article_id, content in articles:
            bias = predict_political_bias(content)
            cursor.execute(
                "UPDATE articles SET bias = %s WHERE id = %s",
                (bias, article_id)
            )

        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def load_data(**context):
    execution_date = context['execution_date']
    sql = """
    INSERT INTO cleared_data (source, url, title, content, bias)
    SELECT source, url, title, content, bias
    FROM articles
    WHERE created_at >= %s AND created_at < %s;
    """
    params = (execution_date - timedelta(days=1), execution_date)
    execute_sql(sql, params)


with DAG('data_transformation', default_args=default_args, schedule_interval='@daily') as dag:
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
    )

    analyze_bias_task = PythonOperator(
        task_id='analyze_bias',
        python_callable=analyze_bias,
        provide_context=True
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

    clean_data_task >> analyze_bias_task >> load_data_task
