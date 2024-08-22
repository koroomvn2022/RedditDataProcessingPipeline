import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etls.aws_etls import *

def aws_s3_pipeline(files_path, partition, file_name):
    # create a s3 session
    s3 = create_s3_session()
    # create a bucket
    bucket = create_s3_bucket(s3)
    # Upload files
    upload_file(bucket, files_path, partition, file_name)
