# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3 
import os

from test_amc import AMCTest, get_aws_account_from_profile, get_cross_region, get_dataset_config, get_cross_account_profile

cfn_client = boto3.client('cloudformation')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
dynamodb_resource = boto3.resource('dynamodb')

STACK_NAME = os.environ['STACK']
REGION = os.environ['AWS_DEFAULT_REGION']
PROFILE = os.environ['AWS_PROFILE']
CUSTOMER_ID = os.environ['CUSTOMER_ID']
TPS_CUSTOMER_TABLE = os.environ['TPS_CUSTOMER_TABLE']
WFM_CUSTOMER_TABLE = os.environ['WFM_CUSTOMER_TABLE']
CLOUDTRAIL_NAME = os.environ['CLOUDTRAIL_NAME']

account_id = get_aws_account_from_profile(profile=PROFILE)
cross_region = get_cross_region()
test_amc_dataset = get_dataset_config()
cross_account_profile = get_cross_account_profile()

default_test = AMCTest(
        deployment_region=REGION,
        orange_account=account_id,
        red_account=account_id,
        customer_id=CUSTOMER_ID + "default",
        stack_name=STACK_NAME,
        profile=PROFILE
        )
cross_region_test = AMCTest(
    deployment_region=cross_region,
    orange_account=account_id,
    red_account=account_id,
    customer_id=CUSTOMER_ID + "crossregion",
    stack_name=STACK_NAME,
    profile=PROFILE
    )
tests = [default_test, cross_region_test]

if cross_account_profile:
    cross_account_test = AMCTest(
        deployment_region=REGION,
        orange_account=account_id,
        red_account=account_id,
        customer_id=CUSTOMER_ID + "crossaccount",
        stack_name=STACK_NAME,
        profile=cross_account_profile
    )
    tests.append(cross_account_test)

for test in tests:
    print(f'\n\n*cleaning {test.customer_id} resources*')
    test.delete_stack()
    test.delete_bucket(bucket_name=test.amc_bucket, profile=test.profile)
    test.clean_table(
        table_name=TPS_CUSTOMER_TABLE, 
        key=test.table_keys['tps_customer_config']
        )
    test.clean_table(
        table_name=WFM_CUSTOMER_TABLE, 
        key=test.table_keys['wfm_customer_config']
        )
    if not test.customer_id.endswith("crossaccount"):
        test.delete_bucket_trail(
            bucket_name=test.amc_bucket,
            trail=f"arn:aws:cloudtrail:{REGION}:{account_id}:trail/{CLOUDTRAIL_NAME}"
            )
    if test_amc_dataset:
        SDLF_CUSTOMER_TABLE = os.environ["SDLF_CUSTOMER_TABLE"]
        STAGE_BUCKET = os.environ["STAGE_BUCKET"]
        test.clean_table(
            table_name=SDLF_CUSTOMER_TABLE,
            key=test.table_keys['sdlf_customer_config']
        )
        test.clean_bucket(bucket_name=STAGE_BUCKET, profile=PROFILE)

