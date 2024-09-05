# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict
from aws_solutions.core.helpers import get_service_client
import aws_solutions.core.config
from aws_solutions.extended.resource_lookup import ResourceLookup
import os
from aws_lambda_powertools import Logger
from cross_account_data_lake import CrossAccountDataLakeTemplate

import boto3

DATA_LAKE_ENABLED = os.environ["DATA_LAKE_ENABLED"]
RESOURCE_PREFIX = os.environ["RESOURCE_PREFIX"]
SNS_KMS_KEY_ID = os.environ["SNS_KMS_KEY_ID"]
APPLICATION_ACCOUNT = os.environ["APPLICATION_ACCOUNT"]
APPLICATION_REGION = os.environ["APPLICATION_REGION"]
ADD_AMC_INSTANCE_LAMBDA_ROLE_ARN = os.environ["ADD_AMC_INSTANCE_LAMBDA_ROLE_ARN"]
TEMPLATE_URL = os.environ["TEMPLATE_URL"]
ARTIFACTS_BUCKET_NAME = os.environ["ARTIFACTS_BUCKET_NAME"]
ARTIFACTS_BUCKET_KEY_ID = os.environ["ARTIFACTS_BUCKET_KEY_ID"]
ROUTING_QUEUE_LOGICAL_ID = os.environ['ROUTING_QUEUE_LOGICAL_ID']
STAGE_A_ROLE_LOGICAL_ID = os.environ['STAGE_A_ROLE_LOGICAL_ID']

logger = Logger(service="AddAMCInstance", level="INFO")


class TpsDeployPatterns:
    def __init__(
            self,
            event: Dict,
    ) -> None:

        self._data_lake_enabled = DATA_LAKE_ENABLED
        self._resource_prefix = RESOURCE_PREFIX
        self._application_account = APPLICATION_ACCOUNT
        self._application_region = APPLICATION_REGION
        self._template_url = TEMPLATE_URL
        self._artifacts_bucket_name = ARTIFACTS_BUCKET_NAME
        self._artifacts_bucket_key_id = ARTIFACTS_BUCKET_KEY_ID

        self._event = event

        # amc
        self._bucket_name = self._event["BucketName"]
        self._orange_room_account = self._event["amcOrangeAwsAccount"]
        self._team_name = self._event["amcTeamName"]

        # s3 bucket
        self._bucket_account = self._event["bucketAccount"]
        self._bucket_region = self._event["bucketRegion"]
        self._bucket_exists = self._event["bucketExists"]

        # sns
        self._create_sns_topic = self._event["createSnsTopic"]
        self._sns_kms_key_id = SNS_KMS_KEY_ID

        # cloudformation
        self.stack_name = '{}-tps-instance-{}'.format(self._resource_prefix, self._event['TenantName'])

        # template params
        self.template_pattern = "amc-initialize.yaml"
        self.template_params = [
            {
                'ParameterKey': 'pBucketName',
                'ParameterValue': self._event['BucketName']
            },
            {
                'ParameterKey': 'pTenantName',
                'ParameterValue': self._event['TenantName']
            },
            {
                'ParameterKey': 'pOrangeRoomAccountId',
                'ParameterValue': self._orange_room_account
            },
            {
                'ParameterKey': 'pLambdaRoleArn',
                'ParameterValue': ADD_AMC_INSTANCE_LAMBDA_ROLE_ARN
            },
            {
                'ParameterKey': 'pDataLakeEnabled',
                'ParameterValue': self._data_lake_enabled
            },
            {
                'ParameterKey': 'pResourcePrefix',
                'ParameterValue': self._resource_prefix
            },
            {
                'ParameterKey': 'pTemplateUrl',
                'ParameterValue': self._template_url
            },
            {
                'ParameterKey': 'pArtifactsBucketName',
                'ParameterValue': self._artifacts_bucket_name
            },
            {
                'ParameterKey': 'pApplicationRegion',
                'ParameterValue': self._application_region
            },
            {
                'ParameterKey': 'pBucketRegion',
                'ParameterValue': self._bucket_region
            }
        ]
        self.cross_account_params = [
            {
                'ParameterKey': 'pBucketAccount',
                'ParameterValue': self._bucket_account
            }
        ]
        self.bucket_status_params = [
            {
                'ParameterKey': 'pBucketExists',
                'ParameterValue': self._bucket_exists
            }
        ]
        self.sns_create_params = [
            {
                'ParameterKey': 'pSnsKeyId',
                'ParameterValue': self._sns_kms_key_id
            }
        ]

    # methods to check customer configuration pattern
    def check_data_lake(self):
        return self._data_lake_enabled == "Yes"

    def check_cross_account(self):
        return self._bucket_account != self._application_account

    def check_cross_region(self):
        return self._bucket_region != self._application_region

    def check_sns_create(self):
        return self._create_sns_topic == "true"

    def check_access_logs_bucket_exists(self):           
        # check target region for access logs bucket stack to get bucket name
        # if the stack doesn't exist we will return None to create the bucket
        cloudformation_client = get_service_client("cloudformation")
        self._access_logs_stack_name = f"{RESOURCE_PREFIX}-tps-{self._bucket_region}-access-logs"
        access_logs_bucket_name = None
        try:
            logger.info(f"Check access logs stack exists in region {self._bucket_region}")
            response = cloudformation_client.describe_stacks(StackName=self._access_logs_stack_name)
            for stack in response['Stacks']:
                for output in stack.get('Outputs', []):
                    if output['OutputKey'] == "S3LogsBucketName":
                        access_logs_bucket_name = output['OutputValue']
        except Exception as e:
            logger.exception(repr(e))
            logger.info(f"Stack name {self._access_logs_stack_name} does not exist in region {self._bucket_region}")
            return None

        # checking just for the presence of the stack isn't enough to guarantee existence of the bucket
        # check for the bucket and if it doesn't exist we will return None to create the bucket
        s3_client = get_service_client("s3")
        if access_logs_bucket_name:
            try:
                response = s3_client.head_bucket(
                    Bucket=access_logs_bucket_name,
                    ExpectedBucketOwner=self._bucket_account
                )
                if response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region'] == self._bucket_region:
                    return access_logs_bucket_name
                else:
                    logger.info(f"Log bucket does not exist in region {self._bucket_region}")
                    return None
            except Exception as e:
                logger.exception(repr(e))
                logger.info(
                    f"Log bucket {access_logs_bucket_name} does not exist in account {self._bucket_account}")
                return None
    
    def check_and_deploy_access_logs_bucket(self):
        access_logs_bucket = self.check_access_logs_bucket_exists()
        if not access_logs_bucket:
            logger.info(f"Deploying amc access logs bucket {access_logs_bucket}")
            self.deploy_stack(
                stack_name=self._access_logs_stack_name,
                region=self._bucket_region,
                parameters=[],
                template_url=f"{TEMPLATE_URL}/amc-bucket-logs.yaml",
            )

    def get_data_lake_params(self):
        self.routing_queue_arn = ResourceLookup(
            logical_id=ROUTING_QUEUE_LOGICAL_ID,
            stack_name=RESOURCE_PREFIX
        ).get_arn(
            resource_type='lambda',
            account_id=APPLICATION_ACCOUNT,
        )

        self.stage_a_role_name = ResourceLookup(
            logical_id=STAGE_A_ROLE_LOGICAL_ID,
            stack_name=RESOURCE_PREFIX).physical_id

        data_lake_params = [
            {
                'ParameterKey': 'pRoutingQueueArn',
                'ParameterValue': self.routing_queue_arn
            },
            {
                'ParameterKey': 'pStageARoleName',
                'ParameterValue': self.stage_a_role_name
            }
        ]
        self.template_params.extend(data_lake_params)

    # methods to deploy customer stacks
    def deploy_stack(
            self,
            stack_name,
            template_url,
            parameters,
            region
    ):
        config = aws_solutions.core.config.botocore_config
        session = boto3.Session(region_name=region)
        cfn = session.client('cloudformation', config=config)
        logger.info(f'Checking if stack {stack_name} exists')
        try:
            stack_resp_desc = cfn.describe_stacks(StackName=stack_name)
        except Exception:
            logger.info('Stack does not exist. Creating stack')
            logger.info(f'Template_params: {parameters}')
            return cfn.create_stack(
                StackName=stack_name,
                TemplateURL=template_url,
                Parameters=parameters,
                Capabilities=["CAPABILITY_NAMED_IAM", "CAPABILITY_IAM"]
                )
        logger.info('Stack exists. Attempting update stack')
        try:
            return cfn.update_stack(
                StackName=stack_name,
                TemplateURL=template_url,
                Parameters=parameters,
                Capabilities=["CAPABILITY_NAMED_IAM", "CAPABILITY_IAM"]
            )
        except Exception as e:
            if ('No updates are to be performed' in str(e)):
                return {"StackId": stack_resp_desc["Stacks"][0]["StackId"]}
            else:
                raise e

    def create_cross_account_data_lake_template(self):
        logger.info("Creating Cfn template for cross-account data lake")
        cross_account_data_lake_role_arn = f"arn:aws:iam::{self._application_account}:role/{self.stage_a_role_name}"
        cross_account_event_bridge_target = f"arn:aws:events:{self._application_region}:{self._application_account}:event-bus/default"

        return CrossAccountDataLakeTemplate(customer_id=self._event['TenantName'],
                                            bucket_name=self._event['BucketName'],
                                            cross_account_data_lake_role_arn=cross_account_data_lake_role_arn,
                                            cross_account_event_bridge_target=cross_account_event_bridge_target,
                                            orange_room_account_id=self._orange_room_account)
