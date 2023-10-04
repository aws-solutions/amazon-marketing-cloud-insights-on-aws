# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
class CrossAccountDataLakeTemplate():
    def __init__(
            self,
            customer_id: str,
            bucket_name: str,
            cross_account_data_lake_role_arn: str,
            cross_account_event_bridge_target: str,
            orange_room_account_id: str
        ):

        self.template_string = '''
        {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": "This template provides cross-account data lake integration. 
                            Deploy this template in the AWS account which contains the 
                            AMC Instance S3 bucket if it differs from the main application account.",
            "Resources": {
                "rS3BucketPolicy": {
                    "Type": "AWS::S3::BucketPolicy",
                    "Properties": {
                        "Bucket":  %s ,
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                "Effect": "Allow",
                                "Principal": {
                                    "AWS":  %s 
                                    },
                                "Action": ["s3:GetObject"],
                                "Resource": "arn:aws:s3:::%s/*"
                                },
                                {
                                "Effect": "Allow",
                                "Principal": {
                                    "AWS": arn:aws:iam::%s:root  
                                    },
                                "Action": ["s3:PutObject", "s3:PutObjectAcl"],
                                "Resource": "arn:aws:s3:::%s/*"
                                }
                            ]
                        }
                    }
                },
                "rRuleExecuteDataLakeRole": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "AssumeRolePolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                "Effect": "Allow",
                                "Principal": {
                                    "Service": "events.amazonaws.com"
                                    },
                                "Action": ["sts:AssumeRole"]
                                }
                            ]
                        },
                        "Policies": [
                            {
                            "PolicyName": "%s-DataLake-Policy",
                            "PolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                    "Effect": "Allow",
                                    "Action": ["events:*"],
                                    "Resource": [%s]
                                    }
                                ]
                            }
                            }
                        ]
                    }
                },
                "rRuleExecuteDataLakeProcess": {
                "Type": "AWS::Events::Rule",
                "Properties": {
                    "Description": "Trigger SDLF data lake process",
                    "State": "ENABLED",
                    "EventPattern": {
                        "source": ["aws.s3"],
                        "detail-type": ["Object Created", "Object Deleted"],
                        "detail": {
                            "bucket": {
                                "name": [%s]
                            }
                        }
                    },
                    "Targets": [
                    {
                        "Arn": %s,
                        "RoleArn": {"Fn::GetAtt" : ["rRuleExecuteDataLakeRole", "Arn"]},
                        "Id": "CrossAccountDataLakeRoutingQueueTrigger"
                    }
                    ]
                }
                }
            },
            "Outputs": {
                "CustomerId": {
                    "Value": %s
                }
            }
        }   
        ''' % (bucket_name, 
               cross_account_data_lake_role_arn, 
               bucket_name,
               orange_room_account_id,
               bucket_name,
               customer_id,
               cross_account_event_bridge_target,
               bucket_name,
               cross_account_event_bridge_target,
               customer_id
               )

