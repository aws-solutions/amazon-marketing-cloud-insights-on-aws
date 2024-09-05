# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from shared_test_modules.dataset_test import DatasetTest

DATASET = "ads_report"
# Import required environment variables. These should be passed in from run-test.sh 
# which calls this file to run the tests.
REGION = os.environ.get('REGION', 'us-east-1')
PROFILE = os.environ.get('STACK_PROFILE', 'default')
try:
    REPORTS_BUCKET = os.environ['REPORTS_BUCKET']
    STAGE_BUCKET = os.environ['STAGE_BUCKET']
    MOCK_DATA = os.environ['MOCK_DATA']
    TEST_TABLE = os.environ['TEST_TABLE']
    TEST_FILENAME = os.environ['TEST_FILENAME']
except KeyError as e:
    raise Exception(f"Missing environment variable: {e.args[0]}") #NOSONAR

def test_ads_report_dataset():
    dataset_test = DatasetTest(
        region=REGION,
        aws_profile=PROFILE,
        raw_bucket=REPORTS_BUCKET,
        stage_bucket=STAGE_BUCKET,
        mock_data_file=MOCK_DATA,
        dataset_name=DATASET,
        raw_object_key=f'adtech/{DATASET}/{TEST_TABLE}/{TEST_FILENAME}.json.gz',
        prestage_object_prefix=f'pre-stage/adtech/{DATASET}/{TEST_TABLE}',
        poststage_object_prefix=f'post-stage/adtech/{DATASET}/{TEST_TABLE}',
    )
    dataset_test.run_test()
