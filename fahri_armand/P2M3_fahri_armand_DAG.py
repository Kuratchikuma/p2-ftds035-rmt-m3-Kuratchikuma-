'''
====================================================================
Milestone 3

Nama  : Fahri Armand Rasyad
Batch : FTDS-035-RMT


Program ini dirancang untuk mengotomatiskan pengambilan data mentah dari PostgreSQL dan melakukan proses pembersihan data, termasuk menghapus duplikasi dan melakukan normalisasi. 
Hasil dari proses ini akan disimpan dalam file CSV yang dinamakan "clean". Setelah data bersih berhasil disimpan, fungsi kedua akan mengirimkan data tersebut ke Elasticsearch, 
di mana data tersebut akan divisualisasikan.

Data yang digunakan merupakan dataset employee yang berisi faktor-faktor yang mempengaruhi kepuasan karyawan seperti lingkungan kerja, stres, jam tidur, dll
====================================================================

'''
import pandas as pd
import psycopg2 as db
import datetime as dt
from datetime import timedelta
from elasticsearch import Elasticsearch

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def retrieve_and_process_data():
    conn_string = "dbname='postgres' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    query = "SELECT * FROM employees;"
    
    # mengambil data dari database
    df = pd.read_sql(query, conn)
    conn.close()

    # hapus data duplikat
    df = df.drop_duplicates()

    # normalisasi kolom
    df.columns = df.columns.str.strip()  # hilangkan spasi dari nama kolom
    df.columns = df.columns.str.lower()  # ubah nama kolom menjadi huruf kecil
    df.columns = df.columns.str.replace(' ', '_')  # ganti spasi dengan underscore
    df.columns = df.columns.str.replace(r'\W', '', regex=True)  # hilangkan simbol non-alfanumerik

    # menangani nilai kosong (missing value)
    for column in df.columns:
        if df[column].dtype in ['float64', 'int64']:
            df[column].fillna(df[column].median(), inplace=True)
        else:
            df[column].fillna('empty', inplace=True)

    # simpan data bersih ke CSV
    df.to_csv('/opt/airflow/dags/P2M3_fahri_armand_clean.csv', index=False)


def send_to_elasticsearch():
    # koneksi ke Elasticsearch
    es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])

    # periksa koneksi Elasticsearch
    if not es.ping():
        print("Gagal terhubung ke Elasticsearch")
        return
    else:
        print("Koneksi ke Elasticsearch berhasil")

    # baca data yang sudah dibersihkan
    df_clean = pd.read_csv('/opt/airflow/dags/P2M3_fahri_armand_clean.csv')

    # kirim data ke Elasticsearch per baris
    for i, row in df_clean.iterrows():
        doc = row.to_dict()  # ubah setiap baris menjadi dictionary
        es.index(index="employee_data", id=i, body=doc)

    print("Data berhasil dikirim ke Elasticsearch")


#_______________________DAG_________________________#

# Konfigurasi DAG
default_args = {
    'owner': 'Fahri',
    'start_date': dt.datetime(2024, 10, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('data_pipeline',
         default_args=default_args,
         schedule_interval="30 6 * * *",  # setiap jam 6:30
         catchup=False
         ) as dag:

    start_message = BashOperator(task_id='start_task',
                                 bash_command='echo "Memulai proses membaca CSV..."')
    
    process_data = PythonOperator(task_id='clean_data', 
                                  python_callable=retrieve_and_process_data)
    
    push_to_elasticsearch = PythonOperator(task_id='send_to_es',
                                           python_callable=send_to_elasticsearch)

start_message >> process_data >> push_to_elasticsearch

