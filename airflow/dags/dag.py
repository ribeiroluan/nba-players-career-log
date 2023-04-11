import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
#from code.player import PlayerCareer, DataCleaner, DataWriter, CleanFolder
#from code.push_to_s3 import UploadToS3
#from code.push_to_redshift import UploadToRedshift
from datetime import datetime, timedelta

schedule_interval = '@daily'
start_date = days_ago(1)
args = {
    'owner': 'luan',
    'start_date': airflow.utils.dates.days_ago(0),
}

with DAG(
    dag_id= 'nba-player-career-log',
    default_args= args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
) as dag:

    extract_data = BashOperator(
        task_id = "extract-data",
        bash_command = "python /opt/airflow/code/main.py" ,
        dag=dag)