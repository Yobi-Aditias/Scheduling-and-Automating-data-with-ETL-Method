import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from elasticsearch import Elasticsearch
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db

# Function to get data from PostgreSQL
def get_data_from_db():
    """function ini digunakan untuk mengambil data dari posgres docker"""
    conn_string = "dbname='milestone3' host='localhost' port='5434' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)  
    df.to_csv('/opt/airflow/dags/P2M3_yobi_ad_data_raw.csv',index=False)
    
def data_pipeline():
    
    """pada funtion ini, digunakan untuk pembersihan data seperti:
    1. mengganti nama kolom, 2. mengganti format pada kolom yang salah.
    Namun untuk kasus kali ini diputuskan untuk tidak melakukan handling missing value
    karena kibana dapat mengatasi hal tersebut, dan supaya datanya lebih murni"""
    
    df_data = pd.read_csv('/opt/airflow/dags/P2M3_yobi_ad_data_raw.csv')
    df_data.columns = df_data.columns.str.lower().str.replace(' ', '_')
    df_data.columns = df_data.columns.str.replace(' ', '_')
    df_data.columns = df_data.columns.str.replace('[\(\)]', '', regex=True)
    df_data.columns = df_data.columns.str.replace('response', 'response_promo') #karena kolom ini bahas promo
    df_data['response_promo'] = df_data['response_promo'].replace({1: 'menerima', 0: 'menolak'})
    df_data['complain'] = df_data['complain'].replace({1: 'komplain', 0: 'tidak komplain'})
    df_data['acceptedcmp1'] = df_data['acceptedcmp1'].replace({1: 'menerima', 0: 'menolak'})
    df_data['acceptedcmp2'] = df_data['acceptedcmp2'].replace({1: 'menerima', 0: 'menolak'})
    df_data['acceptedcmp3'] = df_data['acceptedcmp3'].replace({1: 'menerima', 0: 'menolak'})
    df_data['acceptedcmp4'] = df_data['acceptedcmp4'].replace({1: 'menerima', 0: 'menolak'})
    df_data['acceptedcmp5'] = df_data['acceptedcmp5'].replace({1: 'menerima', 0: 'menolak'})
    df_data['Dt_Customer'] = pd.to_datetime(df_data['Dt_Customer'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
    df_data.to_csv('/opt/airflow/dags/P2M3_yobi_ad_data_clean.csv', index=False)


# Function to post the data to Kibana
def post_to_kibana():
    """function ini digunakan untuk mengupload ke elasticsearch dan kibana"""
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_yobi_ad_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="Customer_Personal_Analyst_M3", id=i+1, body=doc)


# DAG setup
default_args = {
    'owner': 'yobi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(seconds=10),
    #
}

with DAG('Yobi_M3',
         description='pengambilan data, pembersihan, dan upload ke kibana',
         default_args=default_args,
         schedule_interval='@daily', 
         start_date=datetime(2023, 10, 28, 9, 30), # jam 9.30 maka di jakarta yang berada pada gmt+7 akan menjadi 16.30
         catchup=False) as dag:  
        
    
    # mengambil dari sql
    fetch_task = PythonOperator(
        task_id='get_data_from_db',
        python_callable=get_data_from_db
    )
    
    # Task yg akan di eksekusi pythonoperator
    clean_task = PythonOperator(
        task_id='cleaning_data',
        python_callable=data_pipeline
    )
    
     # Task to post to Kibana
    post_to_kibana_task = PythonOperator(
        task_id='post_to_kibana',
        python_callable=post_to_kibana
    )

    fetch_task >> clean_task >> post_to_kibana_task