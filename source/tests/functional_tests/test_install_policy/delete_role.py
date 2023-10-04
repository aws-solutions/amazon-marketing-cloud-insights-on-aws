import os
import sys

import boto3

ROLE_NAME = os.environ['INSTALL_ROLE_NAME']
PROFILE = sys.argv[1]
REGION = sys.argv[2]
POLICY_ARN = sys.argv[3]

session = boto3.session.Session(profile_name=PROFILE)
iam_client = session.client("iam", region_name=REGION)

try:
    # detach policy from role
    response = iam_client.detach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn=POLICY_ARN
    )

    # delete policy 
    response = iam_client.delete_policy(
        PolicyArn=POLICY_ARN
    )

    # delete role
    response = iam_client.delete_role(
        RoleName=ROLE_NAME
    )

except Exception as e:
    print(e)