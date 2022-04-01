from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
import logging
logging.basicConfig(filename='logs.log', level=logging.INFO,
                    format='%(asctime)s: %(levelname)s --> %(funcName)s() --> %(message)s')

default_args = {
    "owner": "yashwanth",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 30),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def insert_data():

    try:
        conn = psycopg2.connect(host="postgres-service-db", database="airflow", user="airflow", password="airflow", port='5432')
        cursor = conn.cursor()
        table = '''CREATE TABLE if not exists dag_execution_time(DAG_ID varchar(250), Execution_Date TIMESTAMPTZ);'''
        cursor.execute(table)
        insert = """insert into dag_execution_time(DAG_ID, Execution_Date)
        select DAG_ID, Execution_Date from dag_run order by Execution_Date desc limit 1;"""
        cursor.execute(insert)
        conn.commit()
        logging.info("Data Insertion Successful")
    except Exception as e:
        logging.info("Error in connection",e)
    finally:
        conn.close()




dag = DAG("Docker_DAG", default_args=default_args, schedule_interval="0 6 * * *")
t1 = PythonOperator(task_id='Inserting_data_to_DB_table', python_callable=insert_data, dag=dag)

t1
