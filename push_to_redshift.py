import boto3
import logging
from botocore import exceptions
from botocore.exceptions import ClientError
from os import getenv
from dotenv import load_dotenv
import sys
import psycopg2
from psycopg2.sql import Identifier, SQL

load_dotenv('.env')
AWS_ID = getenv('AWS_ID')
AWS_KEY = getenv('AWS_KEY')
USERNAME = getenv('REDSHIFT_USERNAME')
PASSWORD = getenv('REDSHIFT_PASSWORD')
PORT = getenv('REDSHIFT_PORT')
DATABASE = getenv('REDSHIFT_DATABASE')
BUCKET_NAME = getenv('BUCKET_NAME')
TABLE_NAME = "curry"
ROLE = getenv('REDSHIFT_ROLE')
HOST  = getenv('REDSHIFT_HOST')


def connect_to_s3():
    try:
        conn = boto3.client('s3',
                        aws_access_key_id = AWS_ID,
                        aws_secret_access_key = AWS_KEY)
        return conn
    except Exception as e:
        print(f"Can't connect to S3. Error: {e}")
        sys.exit(1)

def get_latest_upload_date():
    response = connect_to_s3().list_objects_v2(Bucket=BUCKET_NAME)
    all = response['Contents'] 
    latest = max(all, key=lambda x: x['LastModified'])
    return latest['Key'].split('/')[1]

def get_filepath():
    return f"s3://{BUCKET_NAME}/stephencurry/{get_latest_upload_date()}/regularseason.csv"

def connect_to_redshift():
    """Connect to Redshift instance"""
    try:
        rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host = HOST, port=PORT
        )
        return rs_conn
    except Exception as e:
        print(f"Unable to connect to Redshift. Error {e}")
        sys.exit(1)

def load_data_into_redshift(rs_conn):
    """Load data from S3 into Redshift"""
    with rs_conn:

        cur = rs_conn.cursor()
        cur.execute(sql_drop_old_table)
        cur.execute(sql_create_new_table)
        cur.execute(sql_copy_to_table)

        rs_conn.commit()

def main():
    """Upload file from S3 to Redshift Table"""
    rs_conn = connect_to_redshift()
    load_data_into_redshift(rs_conn)

sql_drop_old_table = f"DROP TABLE IF EXISTS {TABLE_NAME};"
sql_create_new_table = f"""
                    CREATE TABLE {TABLE_NAME} (
                            GAME_ID int PRIMARY KEY,
                            GAME_DATE timestamp,
                            SEASON_TYPE varchar,
                            SEASON_YEAR varchar,
                            PLAYER_ID int,
                            PLAYER_NAME varchar,
                            NICKNAME varchar,
                            TEAM_ID int,
                            TEAM_ABBREVIATION varchar,
                            TEAM_NAME varchar,
                            MATCHUP varchar,
                            WL varchar,
                            MIN float,
                            FGM int,
                            FGA int,
                            FG_PCT float,
                            FG3M int,
                            FG3A int,
                            FG3_PCT float,
                            FTM int,
                            FTA int,
                            FT_PCT float,
                            OREB int,
                            DREB int,
                            REB int,
                            AST int,
                            TOV int,
                            STL int,
                            BLK int,
                            BLKA int,
                            PF int,
                            PFD int,
                            PTS int,
                            PLUS_MINUS int,
                            NBA_FANTASY_PTS float,
                            DD2 bool,
                            TD3 bool
                        );"""
sql_copy_to_table = f'''COPY {TABLE_NAME} (SEASON_YEAR,PLAYER_ID,PLAYER_NAME,NICKNAME,TEAM_ID,TEAM_ABBREVIATION,TEAM_NAME,GAME_ID,GAME_DATE,MATCHUP,WL,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,FTM,FTA,FT_PCT,OREB,DREB,REB,AST,TOV,STL,BLK,BLKA,PF,PFD,PTS,PLUS_MINUS,NBA_FANTASY_PTS,DD2,TD3,SEASON_TYPE)
                    FROM '{get_filepath()}'  iam_role '{ROLE}' IGNOREHEADER 1 DELIMITER ',' CSV;'''

if __name__ == "__main__":
    main()