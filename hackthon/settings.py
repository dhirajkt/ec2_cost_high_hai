import os
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
INSTANCE_ID = os.getenv('INSTANCE_ID')

S3_DATA_BUCKET = os.getenv('S3_DATA_BUCKET')
