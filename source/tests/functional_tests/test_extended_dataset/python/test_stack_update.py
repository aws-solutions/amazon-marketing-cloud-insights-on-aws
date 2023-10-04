import os
import boto3
import time

AWS_PROFILE = os.environ["AWS_PROFILE"]
AWS_REGION = os.environ["AWS_REGION"]
STACK = os.environ["STACK"]
TEMPLATE_URL = os.environ["TEMPLATE_URL"]
EMAIL = os.environ["EMAIL"]

boto3_session = boto3.session.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

cfn_client = boto3_session.client('cloudformation')


def test_update_stack():
    print(f"\nUpdating stack: {STACK}")

    response = cfn_client.update_stack(
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
    )

    assert len(response["StackId"]) > 0

    '''
    Confirm the stack successfully updated
    '''
    stack_status = None

    while stack_status not in ['UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE', 'UPDATE_ROLLBACK_IN_PROGRESS', 'UPDATE_FAILED', 'UPDATE_ROLLBACK_FAILED']:
        print("\nWaiting 3 minutes to check stack update status...")

        time.sleep(3 * 60)

        response = cfn_client.describe_stacks(StackName=STACK)
        stack_status = response['Stacks'][0]['StackStatus']

        print(f"Stack status: {stack_status}")

    assert stack_status == "UPDATE_COMPLETE"
