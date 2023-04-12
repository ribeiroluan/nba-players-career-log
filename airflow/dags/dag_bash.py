import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

"""
DAG to extract NBA data, load into AWS S3, and copy to AWS Redshift using Airflow's bash operator
"""


schedule_interval = '@daily'
start_date = days_ago(1)
args = {'owner': 'luan', 'start_date': start_date, 'retries': 1}

with DAG(
    dag_id= 'nba-player-career-etl',
    description= 'NBA career ETL',
    default_args= args,
    schedule_interval=schedule_interval,
    catchup=True,
    max_active_runs=1,
    tags=['NBA ETL'],
) as dag:

    run_etl = BashOperator(
        task_id = "nba-etl",
        bash_command = "python /opt/airflow/code/main.py" ,
        dag=dag)