from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

dags_folder = Path(__file__).parent
airflow_folder = dags_folder.parent
sys.path.append(str(airflow_folder))
from utils.constants import EXTRACT_PATH

#sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.s3_pipeline import upload_s3_pipeline


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# Define the DAG
with DAG(
    's3_pipeline_dag',
    default_args=default_args,
    description='A simple DAG to run CSV pipeline functions',
    schedule_interval=timedelta(seconds=30),  # Set your schedule interval or leave as None to trigger manually
    start_date=datetime(2025, 2, 8),
    catchup=False,
    concurrency=1,  # Ensures only one task runs at a time in the entire DAG
    max_active_runs=1,  # Ensures only one DAG run happens at a time
) as dag:
    

    # upload to s3
    upload_s3 = PythonOperator(
        task_id='s3_upload',
        python_callable=upload_s3_pipeline,
        dag=dag
    )

    upload_s3