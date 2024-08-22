import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etls.reddit_etl import *

def reddit_pipeline(
        files_path, partition, file_name
        , subreddit, post_fields ,time_filter, limit
        ):
    # create a reddit instance
    reddit = create_reddit_intance()
    # extract posts
    df = extract_posts(reddit, subreddit, post_fields ,time_filter, limit)
    # transform df
    transformed_df = transform_df(df)
    # save df
    save_df(transformed_df, files_path, partition, file_name)

