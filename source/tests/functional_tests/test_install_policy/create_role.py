import json
import os
import sys

import boto3

ROLE_NAME = os.environ['INSTALL_ROLE_NAME']
POLICY_FP = '../../../../IAM_POLICY_INSTALL.json'
PROFILE = sys.argv[1]
REGION = sys.argv[2]

session = boto3.session.Session(profile_name=PROFILE)
iam_client = session.client("iam", region_name=REGION)

# Create role
assume_role_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "cloudformation.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

response = iam_client.create_role(
    RoleName=ROLE_NAME,
    AssumeRolePolicyDocument=json.dumps(assume_role_policy),
    Description="Temporary role for AMC Insights functional tests",
    MaxSessionDuration=43200,

)
role_arn = response['Role']['Arn']

# Create policy
with open(POLICY_FP, "r") as f:
    install_policy = f.read()

response = iam_client.create_policy(
    PolicyName=ROLE_NAME + "Policy",
    PolicyDocument=install_policy,
    Description="Temporary policy for AMC Insights functional tests"
)
policy_arn = response['Policy']['Arn']

# Attach policy to role
response = iam_client.attach_role_policy(
    RoleName=ROLE_NAME,
    PolicyArn=policy_arn
)

# Return policy_arn and role_arn to shell script for use in other scripts
print(f'{policy_arn}%{role_arn}')
