import boto3
import logging
from botocore.exceptions import ClientError
from os import getenv
from dotenv import load_dotenv
import sys
import glob
import logging
import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class UploadToS3:

    def __init__(self):
        """Load AWS credentials"""
        load_dotenv('.env')
        self.aws_id = getenv('AWS_ID')
        self.aws_key = getenv('AWS_KEY')
        self.bucket_name = getenv('BUCKET_NAME')

    def connect_to_s3(self):
        """Connect to S3 Instance"""
        try:
            conn = boto3.client('s3',
                            aws_access_key_id = self.aws_id,
                            aws_secret_access_key = self.aws_key)
            return conn
        except Exception as e:
            print(f"Can't connect to S3. Error: {e}")
            sys.exit(1)

    def create_bucket_if_not_exists(self, conn):
        """Check if bucket exists and create if not"""
        exists = True
        try:
            conn.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                exists = False
        if not exists:
            conn.create_bucket(Bucket=self.bucket_name,  CreateBucketConfiguration={'LocationConstraint': 'sa-east-1'})

    def upload_files_to_s3(self, conn):
        """Upload files to S3 Bucket if they don't already exist there"""
        directory = 'tmp'
        for filename in glob.iglob(f'{directory}/*'):
            exists = True
            player = filename.split('\\', 1)[1].split('-')[0]
            extracted_at = datetime.datetime.strptime(filename.split('\\', 1)[1].split('-')[2][:8], "%Y%m%d").date()
            season_type = filename.split('\\', 1)[1].split('-')[1]
            s3_key = f'{player}/{extracted_at}/{season_type}.csv'

            try:
                #makes sure object does not exist in S3 bucket
                conn.head_object(Bucket=self.bucket_name, Key=s3_key)
                logger.info(f"File {filename} already exists in {self.bucket_name}")
            except Exception as e:
                error_code = e.response["Error"]["Code"]
                if error_code == '404':
                    exists = False
            if not exists:
                conn.upload_file(Filename=filename, Bucket=self.bucket_name, Key=s3_key)
                logger.info(f"File {filename} uploaded to {self.bucket_name} at {datetime.datetime.now()}")

    def upload(self):
        """Upload input file to S3 bucket"""
        conn = self.connect_to_s3()
        self.create_bucket_if_not_exists(conn)
        self.upload_files_to_s3(conn)