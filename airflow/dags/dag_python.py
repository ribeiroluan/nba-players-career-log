import sys
sys.path.append('/opt/airflow/code/')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import player
import push_to_s3
import push_to_redshift
import folder_cleaner

"""
DAG to extract NBA data, load into AWS S3, and copy to AWS Redshift using Airflow's Python operator
"""

def _extract_and_write(player_full_name:str, season_type:str):
    player_obj = player.PlayerCareer(player_full_name=player_full_name, season_type=season_type)
    player.DataWriter(player=player_obj).write()    

def _push_to_s3():
    push_to_s3.UploadToS3().upload()

def _push_to_redshift(season_type:str):
    push_to_redshift.UploadToRedshift(season_type=season_type.lower().replace(' ', '')).copy()

def _clean_temp_folder():
    folder_cleaner.CleanFolder().clean_folder()

args = {'owner': 'luan', 'start_date': days_ago(1), 'retries': 1}

with DAG(
    dag_id= 'nba-player-career-etlv2',
    description= 'NBA career ETL',
    default_args= args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['NBA ETL'],
) as dag:

    task_extract_and_write_regular_season = PythonOperator(
        task_id = "extract-and-write-rs",
        python_callable = _extract_and_write,
        op_kwargs = {'player_full_name':'Stephen Curry', 'season_type':'Regular Season'},
        dag=dag)
    
    task_push_to_s3 = PythonOperator(
        task_id = "push-to-s3",
        python_callable = _push_to_s3,
        dag=dag)

    task_push_to_redshift_regular_season = PythonOperator(
        task_id = "push-to-redshift-rs",
        python_callable = _push_to_redshift,
        op_kwargs = {'season_type':'Regular Season'},
        dag=dag)
    
    task_clean_temp_folder = PythonOperator(
        task_id = "clean-temp-folder",
        python_callable = _clean_temp_folder,
        dag=dag)
    
    task_extract_and_write_playoffs = PythonOperator(
        task_id = "extract-and-write-playoffs",
        python_callable = _extract_and_write,
        op_kwargs = {'player_full_name':'Stephen Curry', 'season_type':'Playoffs'},
        dag=dag)
    
    task_push_to_redshift_playoffs = PythonOperator(
        task_id = "push-to-redshift-playoffs",
        python_callable = _push_to_redshift,
        op_kwargs = {'season_type':'Playoffs'},
        dag=dag)
    
task_extract_and_write_regular_season >> task_push_to_s3
task_extract_and_write_playoffs >> task_push_to_s3
[task_push_to_s3, task_push_to_s3] >> task_push_to_redshift_regular_season >>  task_push_to_redshift_playoffs >> task_clean_temp_folder