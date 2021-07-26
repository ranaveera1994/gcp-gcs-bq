import pandas as pd
from datetime import timedelta, datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery

default_args = {
    "depends_on_past": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "owner": "airflow",
    "retries": 3,
    "relay_delay": timedelta(minutes=2),
    "start_date": day_ago(2)
}

dag = DAG(
    dag_id="cs-bq-dag-2021",
    default_args=default_args,
    catchup=False,
    description="Load files in Storage to BQ",
    max_active_runs=5
)

def combine_all_rows():
    storage_client = storage.Client()
    cs_bucket = storage_client.get_bucket('twitter-stream-data')
    all_blobs = list(cs_bucket.list_blobs())
    df = pd.DataFrame()

    for blob in all_blobs:
        filename = blob.name

        print("\nDownloading filename:", filename)
        blob.download_to_filename(f'/home/airflow/gcs/data/{filename}')         

        print("\nFile downloaded... \n")
        temp_df = pd.read_csv(f'/home/airflow/gcs/data/{filename}')         
        df = df.append(temp_df)
        
        print("Deleting file",filename,"\n")
        blob.delete()

    if len(df) > 0:
        filename = "combined_files.csv"
        df.to_csv(f'/home/airflow/gcs/data/{filename}', index=False)         
        print("File found..\n")
        return "Moving to clean files."
    else:
        print("Files not found.\n")
        return "DAGs will run after files are found."

def process_records():
    df = pd.read_csv("combined_files.csv") 
    df = df[['data.created_at','data.extended_tweet.full_text', 'data.id', 'data.user.id', 'data.user.location', 'data.user.verified', 
        'data.user.followers_count','data.user.time_zone','data.geo','data.lang']]
    df = df.loc[df['data.lang'] == 'eng']
    df.to_csv("cleaned_files", index=False)

def upload_to_bq():
    bq_client = bigquery.Client()
    table_id = "stream-analytics-2021:twitter_stream.twitter_data"

    dst_table = bq_client.get_table(table_id)

    rows_before_insert = dst_table.num_rows
    print("rows before insert: ", rows_before_insert)

    if rows_before_insert > 0:
        disposition = bigquery.WriteDisposition.WRITE_APPEND
        print(f"rows before insert: {rows_before_insert} i.e > 0 disposition is {disposition}")
    elif rows_before_insert == 0:
        disposition = bigquery.WriteDisposition.WRITE_EMPTY
        print(f"rows before insert: {rows_before_insert} i.e = 0 disposition is {disposition}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    uri= "gs://us-central1-twitter-cs-bq-3d39e8a9-bucket/data/processed/cleaned_files.csv"

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    destination_table = client.get_table(table_id)
    print("rows after insert: ", destination_table.num_rows")

start = DummyOperator(task_id="start",dag=dag)
end = DummyOperator(task_id="end",dag=dag)

combine_all_rows_task = PythonOperator(
    task_id='combine_all_rows_task',
    python_callable=combine_all_rows,
    dag=dag
)

clean_and_process_records_task = PythonOperator(
    task_id='process_records_task',
    python_callable=process_records,
    dag=dag
)

upload_to_bigquery_task = PythonOperator(
    task_id='upload_to_bigquery_task',
    python_callable=upload_to_bq,
    dag=dag
)

start >> combine_all_rows_task >> process_records_task >> upload_to_bigquery_task >> end