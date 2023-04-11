import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from code.player import PlayerCareer, DataCleaner, DataWriter, CleanFolder
from code.push_to_s3 import UploadToS3
from code.push_to_redshift import UploadToRedshift
from datetime import datetime, timedelta

player = PlayerCareer(player_full_name="Stephen Curry", season_type='Regular Season')

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

task1 = PythonOperator(
    task_id = "extract-data",
    python_callable = DataWriter(player=player).write(),
    dag=dag)