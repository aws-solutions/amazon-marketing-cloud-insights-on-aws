# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3
import time

AWS_PROFILE = os.environ["AWS_PROFILE"]
REGION = os.environ["REGION"]
STACK = os.environ["STACK"]
TEMPLATE_URL = os.environ["TEMPLATE_URL"]
EMAIL = os.environ["EMAIL"]
ROLE_ARN = os.environ.get("ROLE_ARN")

boto3_session = boto3.session.Session(profile_name=AWS_PROFILE, region_name=REGION)

cfn_client = boto3_session.client('cloudformation')


def test_update_stack():
    print(f"\nUpdating stack: {STACK}")

    cfn_parameters = [
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
    ]
    cfn_capabilities = [
        'CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM', 'CAPABILITY_AUTO_EXPAND'
    ]

    if (not ROLE_ARN) or ROLE_ARN == '0':
        response = cfn_client.update_stack(
            StackName=STACK,
            TemplateURL=TEMPLATE_URL,
            Parameters=cfn_parameters,
            Capabilities=cfn_capabilities,
        )
    else:
        response = cfn_client.update_stack(
            StackName=STACK,
            TemplateURL=TEMPLATE_URL,
            Parameters=cfn_parameters,
            Capabilities=cfn_capabilities,
            RoleARN=ROLE_ARN
        )

    assert len(response["StackId"]) > 0

    '''
    Confirm the stack successfully updated
    '''
    stack_status = None

    while stack_status not in ['UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE', 'UPDATE_ROLLBACK_IN_PROGRESS',
                               'UPDATE_FAILED', 'UPDATE_ROLLBACK_FAILED']:
        print("\nWaiting 3 minutes to check stack update status...")

        time.sleep(3 * 60)

        response = cfn_client.describe_stacks(StackName=STACK)
        stack_status = response['Stacks'][0]['StackStatus']

        print(f"Stack status: {stack_status}")

    assert stack_status == "UPDATE_COMPLETE"
