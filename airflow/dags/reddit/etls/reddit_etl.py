import praw
import pandas as pd
import numpy as np
from pathlib import Path
import yaml

with open('/opt/airflow/dags/reddit/variables.yaml') as file:
    info = yaml.safe_load(file)

CLIENT_ID = info.get('REDDIT', {}).get('CLIENT_ID', {})
CLIENT_SECRET = info.get('REDDIT', {}).get('CLIENT_SECRET', {})
USER_AGENT = info.get('REDDIT', {}).get('USER_AGENT', {})

def create_reddit_intance():
    try:

        reddit = praw.Reddit(
            client_id=CLIENT_ID
            , client_secret=CLIENT_SECRET
            , user_agent=USER_AGENT
        )
        print('Reddit instance created')
    except Exception as e:
        print(f'Task failed: {e}')
        raise

    return reddit


def extract_posts(reddit, subreddit, post_fields ,time_filter, limit):
    try:
        
        subreddit = reddit.subreddit(subreddit)
        data = []

        for post in subreddit.top(time_filter=time_filter, limit=limit):
            postDict = vars(post)
            temp = {field: postDict[field] for field in post_fields}
            data.append(temp)
        print('Data extracted')
    except Exception as e:
        print(f'Task failed: {e}')
        raise

    return pd.DataFrame(data)


def transform_df(df):
    try:
        df['id'] = df['id'].astype(str)
        df['title'] = df['title'].astype(str)
        df['score'] = df['score'].astype(int)
        df['num_comments'] = df['num_comments'].astype(int)
        df['author'] =df['author'].astype(str)
        df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
        df['over_18'] = np.where((df['over_18'] == True), True, False).astype(bool)
        editedMode = df['edited'].mode()
        df['edited'] = np.where((df['edited'].isin([True, False])), df['edited'], editedMode).astype(bool)
        print('Data is transformed')
    except Exception as e:
        print(f'Tasked failed: {e}')
    return df


def save_df(df, files_path, partition, file_name):
    try:
        path = Path(f'{files_path}{partition}')
        path.mkdir(parents=True, exist_ok=True)

        df.to_csv(f'{files_path}{partition}{file_name}.csv', index=False)
        print(f'file uploaded to {path}')
        
    except Exception as e:
        print(f'Task failed: {e}')
        raise