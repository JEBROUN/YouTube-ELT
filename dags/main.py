from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_statistics import (
    get_playlist_id,
    get_video_id,
    extract_video_data,
    save_to_json
)

from datawarehouse.dwh import staging_table, core_table

# Define the local timezone
local_tz = pendulum.timezone("Europe/Paris")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
}

with DAG(
    dag_id="produce_json",
    default_args = default_args,
    description="DAG to produce json file with raw data",
    schedule="0 19 * * *",
    catchup=False,
) as dag :

    playlistId = get_playlist_id()
    video_ids = get_video_id(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(video_data)

    playlistId >> video_ids >> video_data >> save_to_json_task


# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule="0 20 * * *",
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies
    update_staging >> update_core 

