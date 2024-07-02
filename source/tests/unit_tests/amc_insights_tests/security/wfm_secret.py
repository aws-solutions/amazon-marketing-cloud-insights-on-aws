# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This module is responsible for testing the WFM Secret security in the AMC Insights stack.
"""


def check_statements(logical_id, resource):
    secret_resource = {
            "Fn::Join": [
                "",
                [
                "arn:aws:secretsmanager:",
                {
                "Ref": "AWS::Region"
                },
                ":",
                {
                "Ref": "AWS::AccountId"
                },
                ":secret:",
                {
                "Fn::Join": [
                    "-",
                    [
                    {
                    "Fn::Select": [
                    0,
                    {
                        "Fn::Split": [
                        "-",
                        {
                        "Fn::Select": [
                        6,
                        {
                            "Fn::Split": [
                            ":",
                            {
                            "Ref": "wfmSecret80634C81"
                            }
                            ]
                        }
                        ]
                        }
                        ]
                    }
                    ]
                    },
                    {
                    "Fn::Select": [
                    1,
                    {
                        "Fn::Split": [
                        "-",
                        {
                        "Fn::Select": [
                        6,
                        {
                            "Fn::Split": [
                            ":",
                            {
                            "Ref": "wfmSecret80634C81"
                            }
                            ]
                        }
                        ]
                        }
                        ]
                    }
                    ]
                    }
                    ]
                ]
                },
                "*"
                ]
            ]
        }
    
    statements = resource["Properties"]["PolicyDocument"].get("Statement", [])
    for statement in statements:
        if statement["Effect"] == "Allow": 
            assert statement["Resource"] != secret_resource, f"Policy {logical_id} should not have permission to access wfmSecret80634C81"

def test_wfm_secrets_security(template):
    # test that only the wfm lambdas and admin policy have access to our WFM Secret    
    policies = template.find_resources("AWS::IAM::Policy")
    managed_policies = template.find_resources("AWS::IAM::ManagedPolicy")
    
    allowed_policy_ids = [
        "wfmSecretsManagerLambdaIamPolicyE74CB487",
        "adminpolicyMicroserviceAdminPolicyE18CA295",
        "adminpolicyDataLakeAdminPolicy73C5E464",
    ]

    for policy_logical_id, policy_resource in {**policies, **managed_policies}.items():
        if policy_logical_id not in allowed_policy_ids:
            check_statements(logical_id=policy_logical_id, resource=policy_resource)
