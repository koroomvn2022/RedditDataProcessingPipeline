import boto3
import yaml

with open('/opt/airflow/dags/reddit/variables.yaml') as file:
    info = yaml.safe_load(file)

AWS_ACCESS_KEY_ID = info.get('AWS', {}).get('AWS_ACCESS_KEY_ID', {})
AWS_SECRET_ACCESS_KEY = info.get('AWS', {}).get('AWS_SECRET_ACCESS_KEY', {})

def create_s3_session():
    try:
        sessionS3 = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID
            , aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        print(f'Tasked failed: {e}')
        raise
    s3 = sessionS3.resource('s3')
    return s3

def create_s3_bucket(s3):
    bucket = s3.Bucket('reddit-koroomvn-bucket')
    try:
        if not bucket in s3.buckets.all():
            bucket.create(
                CreateBucketConfiguration={
                    'LocationConstraint': 'ap-southeast-1'
                }
            )
            print('Bucket created')
        else:
            print('Bucket already exists')
            
    except Exception as e:
        print(f'Task failed: {e}')
        raise

    return bucket

def upload_file(bucket, files_path, partition, file_name):
    
    try:
        bucket.upload_file(
            Filename=f'{files_path}{partition}{file_name}.csv'
            , Key=f'raw/{partition}{file_name}.csv'
        )
        print(f'File uploaded')
    except Exception as e:
        print(f'Task failed; {e}')
        raise