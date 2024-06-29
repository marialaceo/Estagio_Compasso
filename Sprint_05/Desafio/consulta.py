import boto3
from dotenv import load_dotenv
import os

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
response = s3_client.list_buckets()
print('Buckets S3:')
for bucket in response['Buckets']:
    bucket_name = bucket["Name"]
    print(f'  {bucket_name}')

response = s3_client.list_objects_v2(Bucket=bucket_name)
if 'Contents' in response:
    print(f'Files in bucket "{bucket_name}":')
    for obj in response['Contents']:
        file_name = obj["Key"]
        print(f'  {file_name}')
else:
    print(f'No files found in bucket "{bucket_name}".')

query = """
SELECT *
FROM s3object s
    WHERE
        MAX(S.CODIGO_EMEC_IES_BOLSA) > 200
        OR
        s.RACA_BENEFICIARIO = 'Parda'
        AND
        LOWER(s.SEXO_BENEFICIARIO) = 'f'
        AND
        s.REGIAO_BENEFICIARIO = 'NORDESTE'
LIMIT 5
"""

response = s3_client.select_object_content(
    Bucket=bucket_name,
    Key=file_name,
    ExpressionType='SQL',
    Expression=query,
    InputSerialization={'CSV': {"FileHeaderInfo": "USE", "AllowQuotedRecordDelimiter": True, 'FieldDelimiter': ',', 'RecordDelimiter': '\n'}},
    OutputSerialization={'CSV': {}},
)

result = ""
for event in response['Payload']:
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8', errors='ignore')
        result += records

print(result)