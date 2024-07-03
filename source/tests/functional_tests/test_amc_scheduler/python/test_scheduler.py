# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3
import json
import time
import pytest

STACK = os.environ['STACK']
STACK_REGION = os.environ['STACK_REGION']
STACK_PROFILE = os.environ['STACK_PROFILE']
CREATE_SCHEDULE_LAMBDA = os.environ['CREATE_SCHEDULE_LAMBDA']
DELETE_SCHEDULE_LAMBDA = os.environ['DELETE_SCHEDULE_LAMBDA']
RULE_SUFFIX = os.environ['RULE_SUFFIX']
RULE_NAME = f"{STACK}-wfm-{RULE_SUFFIX}"

session = boto3.session.Session(profile_name=STACK_PROFILE, region_name=STACK_REGION)
lambda_client = session.client('lambda')
events_client = session.client('events')

def test_create_schedule():
    # invoke lambda function to create workflow schedule
    print('Invoking CREATE WORKFLOW SCHEDULE lambda function')

    lambda_client.invoke(
        FunctionName=CREATE_SCHEDULE_LAMBDA,
        InvocationType='RequestResponse',
        Payload=json.dumps({
            "execution_request": {
                "customerId": "test",
                "requestType": "createExecution",
                "createExecutionRequest": {"workflowId": "test"}
            },
            "schedule_expression": "cron(0/15 * * * ? *)",
            "rule_name": RULE_SUFFIX,
            "rule_description": "testing amc workflow scheduler"
        })
    )

    print('Waiting 30 seconds for function to run')
    time.sleep(30)

    # check events to see if schedule is created
    print('Checking for successful creation of rule')

    try:
        response = events_client.describe_rule(
            Name=RULE_NAME
        )
    except Exception:
        response = None
    
    assert response

def test_delete_schedule():
    # invoke lambda function to delete workflow schedule
    print('Invoking DELETE WORKFLOW SCHEDULE lambda function')

    lambda_client.invoke(
        FunctionName=DELETE_SCHEDULE_LAMBDA,
        InvocationType='RequestResponse',
        Payload=json.dumps({
            "rule_name": RULE_SUFFIX
        })
    )

    # check events to see if schedule is deleted
    print('Checking for successful deletion of rule')

    try:
        response = events_client.describe_rule(
            Name=RULE_NAME
        )
    except Exception:
        response = None
        
    assert not response
