# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3

STACK = os.environ['STACK']
STACK_REGION = os.environ['STACK_REGION']
STACK_PROFILE = os.environ['STACK_PROFILE']
RULE_SUFFIX = os.environ['RULE_SUFFIX']
RULE_NAME = f"{STACK}-wfm-{RULE_SUFFIX}"

session = boto3.Session(profile_name=STACK_PROFILE, region_name=STACK_REGION)
events_client = session.client('events')

# Delete rule if it exists
try:
    response = events_client.delete_rule(
        Name=RULE_NAME
    )
    print(response)
except Exception:
    pass
