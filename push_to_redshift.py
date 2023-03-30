import boto3
import logging
from os import getenv
from dotenv import load_dotenv
import sys
import psycopg2
import logging
import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class UploadToRedshift:

    def __init__(self):
        """Load AWS credentials"""
        load_dotenv('.env')
        self.aws_id = getenv('AWS_ID')
        self.aws_key = getenv('AWS_KEY')
        self.username = getenv('REDSHIFT_USERNAME')
        self.password = getenv('REDSHIFT_PASSWORD')
        self.port = getenv('REDSHIFT_PORT')
        self.database = getenv('REDSHIFT_DATABASE')
        self.bucket_name = getenv('BUCKET_NAME')
        self.table_name = "curry"
        self.role = getenv('REDSHIFT_ROLE')
        self.host  = getenv('REDSHIFT_HOST')

    def connect_to_s3(self):
        try:
            conn = boto3.client('s3',
                            aws_access_key_id = self.aws_id,
                            aws_secret_access_key = self.aws_key)
            return conn
        except Exception as e:
            print(f"Can't connect to S3. Error: {e}")
            sys.exit(1)

    def get_latest_upload_date(self):
        response = self.connect_to_s3().list_objects_v2(Bucket=self.bucket_name)
        all = response['Contents'] 
        latest = max(all, key=lambda x: x['LastModified'])
        return latest['Key'].split('/')[1]

    def get_filepath(self):
        return f"s3://{self.bucket_name}/stephencurry/{self.get_latest_upload_date()}/regularseason.csv"

    def connect_to_redshift(self):
        """Connect to Redshift instance"""
        try:
            rs_conn = psycopg2.connect(
                dbname=self.database, user=self.username, password=self.password, host = self.host, port=self.port
            )
            return rs_conn
        except Exception as e:
            print(f"Unable to connect to Redshift. Error {e}")
            sys.exit(1)

    def sql_drop_old_table(self):
        return f"DROP TABLE IF EXISTS {self.table_name};"
        
    def sql_create_new_table(self):
        return f""" CREATE TABLE {self.table_name} (
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
                    TD3 bool);"""
    
    def sql_copy_to_table(self):
        return f""" COPY {self.table_name} (SEASON_YEAR,PLAYER_ID,PLAYER_NAME,NICKNAME,TEAM_ID,TEAM_ABBREVIATION,TEAM_NAME,GAME_ID,GAME_DATE,MATCHUP,WL,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,FTM,FTA,FT_PCT,OREB,DREB,REB,AST,TOV,STL,BLK,BLKA,PF,PFD,PTS,PLUS_MINUS,NBA_FANTASY_PTS,DD2,TD3,SEASON_TYPE)
                    FROM '{self.get_filepath()}'  iam_role '{self.role}' IGNOREHEADER 1 DELIMITER ',' CSV;""" 

    def load_data_into_redshift(self, rs_conn):
        """Load data from S3 into Redshift"""
        with rs_conn:
            try:
                cur = rs_conn.cursor()
                cur.execute(self.sql_drop_old_table())
                cur.execute(self.sql_create_new_table())
                cur.execute(self.sql_copy_to_table())
                rs_conn.commit()
                logger.info(f"S3 file {self.get_filepath()} uploaded to Redshift at {datetime.datetime.now()}")
            except:
                logger.info(f"Error copying S3 file {self.get_filepath()} to Redshift at {datetime.datetime.now()}")
                
    def copy(self):
        """Upload file from S3 to Redshift Table"""
        rs_conn = self.connect_to_redshift()
        self.load_data_into_redshift(rs_conn)