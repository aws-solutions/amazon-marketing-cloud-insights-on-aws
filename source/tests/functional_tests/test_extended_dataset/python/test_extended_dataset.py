# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3
import time
from .configure_extended_dataset import TestDatasetParameters

AWS_PROFILE = os.environ["AWS_PROFILE"]
MOCK_DATA_DIR = os.environ["MOCK_DATA_DIR"]
STACK = os.environ["STACK"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
STAGE_BUCKET = os.environ["STAGE_BUCKET"]
dataset = TestDatasetParameters.dataset
team = 'adtech'

boto3_session = boto3.session.Session(profile_name=AWS_PROFILE)
s3 = boto3_session.resource('s3')
s3_client = boto3_session.client('s3')


def upload_data(data_file, raw_bucket, object_key):
    print(f'\n\nUploading test data to bucket: {RAW_BUCKET}\nObject: {object_key}')
    try:
        with open(data_file, "rb") as f:
            s3_client.upload_fileobj(f, raw_bucket, object_key)
    except Exception as e:
        print(e)


def stage_testing(stage_bucket, prefix):
    status = "ERROR"
    tries = 0
    while ((status == "ERROR") and (tries <= 2)):
        tries += 1
        try:
            print(f'\nattempt # {tries} of 3')
            print('waiting 60 seconds for data to post...')
            time.sleep(60)
            print(f'checking bucket {stage_bucket} for file')
            response = s3_client.list_objects(Bucket=stage_bucket, Prefix=prefix)
            if response['Contents']:
                status = "SUCCESS"
        except KeyError:
            print('file not found')
            status = "ERROR"
        except Exception as e:
            print(f'error: {e}')
            status = "ERROR"

    print(f"stage status: {status}")

    assert status == "SUCCESS"


def test_datalake():
    object_prefix = f"{team}/{dataset}"
    upload_data(f"{MOCK_DATA_DIR}/memberships.json", RAW_BUCKET, f"{object_prefix}/memberships.json")

    print('\n*starting dataset stage a tests*')
    stage_testing(
        stage_bucket=STAGE_BUCKET,
        prefix=f'pre-stage/adtech/{dataset}/'
    )

    print("\nwaiting 5 minutes for heavy transform glue job to run...")
    time.sleep(5 * 60)

    print('\n*starting dataset stage b tests*')
    stage_testing(
        stage_bucket=STAGE_BUCKET,
        prefix=f'post-stage/adtech/{dataset}/'
    )
