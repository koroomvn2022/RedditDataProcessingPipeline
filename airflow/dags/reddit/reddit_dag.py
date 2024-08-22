import yaml
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipelines.reddit_pipeline import reddit_pipeline
from pipelines.aws_s3_pipeline import aws_s3_pipeline


# TODO: Get information 

with open('/opt/airflow/dags/reddit/variables.yaml') as file:
    info = yaml.safe_load(file)


SUBREDDIT = 'dataengineering'
LIMIT = info.get(SUBREDDIT, {}).get('LIMIT', {})
TIME_FILTER= info.get(SUBREDDIT, {}).get('TIME_FILTER', {})
POST_FIELDS = info.get(SUBREDDIT, {}).get('POST_FIELDS', {})

TODAY = datetime.date.today()
YEAR = TODAY.year
MONTH = TODAY.month
DAY = TODAY.day
FILES_PATH = '/opt/airflow/dags/reddit/output/'
PARTITION = f'year={YEAR}/month={MONTH}/day={DAY}/'
FILE_NAME = f'reddit_{TODAY.strftime('%Y%m%d')}'

#TODO: Create a dag

defaultArgs = {
    'owner': 'admin'
    , 'catchup': False
}

with DAG(
    dag_id='reddit_dag_v1'
    , start_date=datetime.datetime.now()
    , schedule_interval='@daily'
    , default_args=defaultArgs
    , catchup=False
    , tags=['reddit', 'etl', 'pipeline']
) as dag:
    
    create_reddit_pipeline = PythonOperator(
        task_id='reddit_pipeline'
        , python_callable=reddit_pipeline
        , op_kwargs={
            'files_path': FILES_PATH
            , 'partition': PARTITION
            , 'file_name': FILE_NAME
            , 'subreddit': SUBREDDIT
            , 'post_fields': POST_FIELDS
            , 'time_filter': TIME_FILTER
            , 'limit': LIMIT
        }
        , dag=dag
    )

    create_aws_pipeline = PythonOperator(
        task_id='aws_pipeline'
        , python_callable=aws_s3_pipeline
        , op_kwargs={
            'files_path': FILES_PATH
            , 'partition': PARTITION
            , 'file_name': FILE_NAME
        }
        , dag=dag
    )

    create_reddit_pipeline >> create_aws_pipeline
