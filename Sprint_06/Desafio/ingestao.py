import boto3
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
region_name = os.getenv('AWS_REGION')

s3_client = boto3.client('s3', 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key,
                      aws_session_token=aws_session_token,
                      region_name=region_name
                      )

def upload_arquivo_s3(file_path, bucket, key):
    try:
        s3_client.upload_file(file_path, bucket, key)
        print(f'Sucesso no upload de {file_path} para {bucket}/{key}')
    except Exception as e:
        print(f'Erro no upload {file_path}: {e}')

def main():
    bucket_name = 'data-lake-final-programa-de-bolsas'
    raw_zone = 'Raw/Files/CSV'

    now = datetime.now()
    date_path = now.strftime('%Y/%m/%d')

    files = {
        'movies.csv': 'Movies',
        'series.csv': 'Series'
    }

    for file_name, category in files.items():
        file_path = os.path.join(os.getcwd(), file_name)
        s3_key = f'{raw_zone}/{category}/{date_path}/{file_name}'
        upload_arquivo_s3(file_path, bucket_name, s3_key)

if __name__ == "__main__":
    main()