# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3
import time

AWS_PROFILE = os.environ["AWS_PROFILE"]
AWS_REGION = os.environ["REGION"]
STACK = os.environ["STACK"]
TEMPLATE_URL = os.environ["TEMPLATE_URL"]
EMAIL = os.environ["EMAIL"]
ROLE_ARN = os.environ["ROLE_ARN"]

boto3_session = boto3.session.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

cfn_client = boto3_session.client('cloudformation', region_name=AWS_REGION)


def test_create_stack():
    print(f"\nCreating stack in region {AWS_REGION}: {STACK}")

    response = cfn_client.create_stack(
        StackName=STACK,
        TemplateURL=TEMPLATE_URL,
        Parameters=[
            {
                'ParameterKey': 'NotificationEmail',
                'ParameterValue': EMAIL,
            },
            {
                'ParameterKey': 'ShouldDeployDataLake',
                'ParameterValue': "Yes",
            },
            {
                'ParameterKey': 'ShouldDeployMicroservices',
                'ParameterValue': "Yes",
            },
        ],
        Capabilities=[
            'CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM', 'CAPABILITY_AUTO_EXPAND'
        ],
        RoleARN=ROLE_ARN
    )

    assert len(response["StackId"]) > 0

    '''
    Confirm the stack successfully deployed
    '''
    stack_status = None

    while stack_status not in ['CREATE_COMPLETE', 'ROLLBACK_COMPLETE', 'ROLLBACK_IN_PROGRESS', 'CREATE_FAILED']:
        print("\nWaiting 3 minutes to check stack creation status...")

        time.sleep(3 * 60)

        response = cfn_client.describe_stacks(StackName=STACK)
        stack_status = response['Stacks'][0]['StackStatus']

        print(f"Stack status: {stack_status}")

    assert stack_status == "CREATE_COMPLETE"
