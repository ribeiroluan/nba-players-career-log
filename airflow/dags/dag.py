import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.player import PlayerCareer, DataCleaner, DataWriter, CleanFolder
from scripts.push_to_s3 import UploadToS3
from scripts.push_to_redshift import UploadToRedshift
from datetime import datetime, timedelta

player = PlayerCareer(player_full_name="Stephen Curry", season_type='Regu')

args = {
    'owner': 'luan',
    'start_date': airflow.utils.dates.days_ago(0),
}

dag = DAG(
    dag_id= 'nba-player-career-log',
    default_args= args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
)