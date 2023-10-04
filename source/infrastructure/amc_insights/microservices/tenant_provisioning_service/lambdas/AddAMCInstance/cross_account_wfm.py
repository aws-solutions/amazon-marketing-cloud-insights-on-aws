# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
class CrossAccountWfmTemplate():
    def __init__(
            self,
            customer_id: str,
            cross_account_api_role_name: str,
            application_account_id: str,
            amc_api_id: str,
            amc_region: str
        ):

        self.template_string = '''
        {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": "This template provides cross-account API authentication. 
                            Deploy this template in the AMC Connected AWS Account if it 
                            differs from the main application account.",
            "Resources": {
                "rApiInvokeRoleCrossAccount": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": "arn:aws:iam::%s:root"
                        },
                        "Action": ["sts:AssumeRole"]
                        }
                    ]
                    },
                    "Policies": [
                    {
                        "PolicyName": "%s-Policy",
                        "PolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                            "Effect": "Allow",
                            "Action": ["execute-api:Invoke"],
                            "Resource": ["arn:aws:execute-api:%s:*:%s/*"]
                            }
                        ]
                        }
                    }
                    ],
                    "RoleName": %s
                }
                }
            },
            "Outputs": {
                "CustomerId": {
                    "Value": %s
                }
            }
        }
        '''  % (application_account_id, 
                cross_account_api_role_name,
                amc_region,
                amc_api_id,
                cross_account_api_role_name,
                customer_id
                )
    
