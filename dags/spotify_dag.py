from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

dags_folder = Path(__file__).parent
airflow_folder = dags_folder.parent
sys.path.append(str(airflow_folder))
from utils.constants import INPUT_PATH, OUTPUT_PATH,EXTRACT_PATH

unp_csv = INPUT_PATH
pro_csv = OUTPUT_PATH
final_csv = EXTRACT_PATH

from pipelines.csv_pipline import fetch_first_entry, remove_first_entry, add_first_entry_to_another_csv
from pipelines.spotify_pipeline import get_tracks_for_artist, fetch_album_tracks, fetch_artist_albums

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# Define the DAG
with DAG(
    'csv_pipeline_dag',
    default_args=default_args,
    description='A simple DAG to run CSV pipeline functions',
    schedule_interval=timedelta(seconds=30),
    start_date=datetime(2025, 2, 8),
    catchup=False,
    concurrency=1, 
    max_active_runs=1, 
) as dag:

    # Task 1: Fetch the first entry from the CSV
    task_fetch_first_entry = PythonOperator(
        task_id='fetch_first_entry',
        python_callable=fetch_first_entry,
        op_args=[unp_csv],  # Pass the CSV file path
    )

    #Task 2
    task_extract_track = PythonOperator(
        task_id="extract_spotify_data",
        python_callable=get_tracks_for_artist,
        provide_context=True,
        op_args=[unp_csv],  # Pass the CSV file path
        dag=dag,
    )

    # Task 3: Remove the first entry from the CSV
    task_remove_first_entry = PythonOperator(
        task_id='remove_first_entry',
        python_callable=remove_first_entry,
        op_args=[unp_csv],  # Pass the CSV file path
    )
    
    # Task 4: Add the first entry to another CSV
    task_add_first_entry_to_another_csv = PythonOperator(
        task_id='add_first_entry_to_another_csv',
        python_callable=add_first_entry_to_another_csv,
        op_args=[unp_csv, pro_csv],  # Pass both CSV file paths
    )

    # Task dependencies
    task_fetch_first_entry >>  task_add_first_entry_to_another_csv >> task_extract_track  >> task_remove_first_entry   
