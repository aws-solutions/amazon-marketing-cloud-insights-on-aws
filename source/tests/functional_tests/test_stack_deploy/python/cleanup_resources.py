# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3
from botocore.exceptions import ClientError
import time

AWS_PROFILE = os.environ["AWS_PROFILE"]
STACK_NAME = os.environ["STACK"]

boto3_session = boto3.session.Session(profile_name=AWS_PROFILE)

s3 = boto3_session.resource('s3')
s3_client = boto3_session.client('s3')
cfn_client = boto3_session.client('cloudformation')


def cleanup_stacks():
    # clean up cloudformation stacks
    try:
        print(f'\nDeleting stack {STACK_NAME}')
        cfn_client.delete_stack(StackName=STACK_NAME)

    except Exception as e:
        print(f"Error deleting stack {STACK_NAME}: {e}")

    stacks_deleting_in_progress = cfn_client.list_stacks(StackStatusFilter=['DELETE_IN_PROGRESS'])['StackSummaries']
    stack_names_deleting_in_progress = [str(item['StackName']) for item in stacks_deleting_in_progress if
                                        str(item['StackName']) == STACK_NAME]

    while len(stack_names_deleting_in_progress) > 0:
        print("Stack status is DELETE_IN_PROGRESS")
        print("\nWaiting 2 minutes to check stack deletion status...")
        time.sleep(2 * 60)
        try:
            stacks_deleting_in_progress = cfn_client.list_stacks(StackStatusFilter=['DELETE_IN_PROGRESS'])[
                'StackSummaries']
            stack_names_deleting_in_progress = [str(item['StackName']) for item in stacks_deleting_in_progress]
        except Exception as error:
            print(error)
            break

    '''
    Check the stack successfully deleted
    '''
    try:
        response = cfn_client.describe_stacks(StackName=STACK_NAME)
        stack_status = response['Stacks'][0]['StackStatus']
        print(f"Stack status: {stack_status}, manually stack deletion is required")
    except ClientError as err:
        print("\nDelete stack successfully")


def cleanup_buckets():
    response = s3_client.list_buckets()

    bucket_names: list[str] = []
    for item in response['Buckets']:
        bucket_name: str = str(item['Name'])
        if bucket_name.startswith(STACK_NAME):
            bucket_names.append(bucket_name)

    for bucket_name in bucket_names:
        try:
            bucket = s3.Bucket(bucket_name)
            bucket.object_versions.delete()
            bucket.objects.all().delete()
            s3_client.delete_bucket(Bucket=bucket_name)
            print(f"Delete {bucket_name} successfully")
        except Exception as err:
            print(f'Error deleting bucket:\n{err}')
            continue


print('\n*Cleaning test stack*')
cleanup_stacks()

print('\n*Cleaning test buckets*')
cleanup_buckets()
