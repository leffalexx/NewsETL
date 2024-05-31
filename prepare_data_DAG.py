from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from bias_analysis import predict_political_bias
import mysql.connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('data_transformation', default_args=default_args, schedule_interval='@daily') as dag:
    clean_data_task = MySqlOperator(
        task_id='clean_data',
        mysql_conn_id='mysql_conn',  
        sql="""
            DELETE FROM articles
            WHERE title = '' OR content = '';
            DELETE FROM articles
            WHERE LENGTH(url) < 40;
        """
    )

def analyze_bias(**kwargs):
    mysql_conn_id = kwargs.get('mysql_conn_id')
    conn = mysql.connector.connect(...)
    cursor = conn.cursor()
    cursor.execute("SELECT content FROM articles")
    contents = [row[0] for row in cursor.fetchall()]
    biases = []
    for content in contents:
        bias = predict_political_bias(content)
        biases.append(bias)
    cursor.executemany(
        "UPDATE articles SET bias = %s WHERE content = %s",
        [(bias, content) for bias, content in zip(biases, contents)]
    )
    conn.commit()
    cursor.close()
    conn.close()

    analyze_bias_task = PythonOperator(
        task_id='analyze_bias',
        python_callable=analyze_bias,
        op_kwargs={
            'mysql_conn_id': 'mysql_conn',
        }
    )

    load_data_task = MySqlOperator(
        task_id='load_data',
        mysql_conn_id='mysql_conn',
        sql="""
            INSERT INTO cleared_data (source, url, title, content, bias)
            SELECT source, url, title, content, bias
            FROM (
                SELECT source, url, title, content,
                       predict_political_bias(content) AS bias
                FROM articles
            ) AS processed_data;
        """
    )

    clean_data_task >> analyze_bias_task >> load_data_task
    