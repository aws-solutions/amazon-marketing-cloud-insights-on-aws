# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from shared_test_modules.dataset_clean import DatasetClean


try:
    REPORTS_BUCKET = os.environ['REPORTS_BUCKET']
    STAGE_BUCKET = os.environ['STAGE_BUCKET']
    MOCK_DATA = os.environ['MOCK_DATA']
    TEST_TABLE = os.environ['TEST_TABLE']
    TEST_FILENAME = os.environ['TEST_FILENAME']
    REGION = os.environ.get('REGION', 'us-east-1')
    PROFILE = os.environ.get('STACK_PROFILE', 'default')
    DATASET = "ads_report"
    
    test_cleanup = DatasetClean(
        region=REGION,
        aws_profile=PROFILE,
        raw_bucket=REPORTS_BUCKET,
        stage_bucket=STAGE_BUCKET,
        raw_object_key=f'adtech/{DATASET}/{TEST_TABLE}/{TEST_FILENAME}.json',
        prestage_object_prefix=f'pre-stage/adtech/{DATASET}/{TEST_TABLE}',
        poststage_object_prefix=f'post-stage/adtech/{DATASET}/{TEST_TABLE}'
    )
    test_cleanup.clean_s3_tests()

except Exception as e:
    print(f"Error cleaning tests: {e}")
    raise
