# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict
import re
from aws_solutions.core.helpers import get_service_client
import aws_solutions.core.config
from aws_solutions.extended.resource_lookup import ResourceLookup
import os
from aws_lambda_powertools import Logger
from cross_account_data_lake import CrossAccountDataLakeTemplate
from cross_account_wfm import CrossAccountWfmTemplate

import boto3

DATA_LAKE_ENABLED = os.environ["DATA_LAKE_ENABLED"]
RESOURCE_PREFIX = os.environ["RESOURCE_PREFIX"]
WFM_LAMBDA_ROLE_NAMES = os.environ["WFM_LAMBDA_ROLE_NAMES"]
SNS_KMS_KEY_ID = os.environ["SNS_KMS_KEY_ID"]
APPLICATION_ACCOUNT = os.environ["APPLICATION_ACCOUNT"]
APPLICATION_REGION = os.environ["APPLICATION_REGION"]
ADD_AMC_INSTANCE_LAMBDA_ROLE_ARN = os.environ["ADD_AMC_INSTANCE_LAMBDA_ROLE_ARN"]
LOGGING_BUCKET_NAME = os.environ["LOGGING_BUCKET_NAME"]
TEMPLATE_URL = os.environ["TEMPLATE_URL"]
ARTIFACTS_BUCKET_NAME = os.environ["ARTIFACTS_BUCKET_NAME"]
ARTIFACTS_BUCKET_KEY_ID = os.environ["ARTIFACTS_BUCKET_KEY_ID"]
API_INVOKE_ROLE_STANDARD = os.environ["API_INVOKE_ROLE_STANDARD"]
ROUTING_QUEUE_LOGICAL_ID = os.environ['ROUTING_QUEUE_LOGICAL_ID']
STAGE_A_ROLE_LOGICAL_ID = os.environ['STAGE_A_ROLE_LOGICAL_ID']


logger = Logger(service="AddAMCInstance", level="INFO")
cloudwatch_client = get_service_client

class TpsDeployPatterns():
    def __init__(
        self,
        event: Dict,
    ) -> None:

        self._data_lake_enabled = DATA_LAKE_ENABLED
        self._resource_prefix = RESOURCE_PREFIX
        self._wfm_lambda_roles_names = WFM_LAMBDA_ROLE_NAMES
        self._application_account = APPLICATION_ACCOUNT
        self._application_region = APPLICATION_REGION
        self._logging_bucket_name = LOGGING_BUCKET_NAME
        self._template_url = TEMPLATE_URL
        self._artifacts_bucket_name = ARTIFACTS_BUCKET_NAME
        self._artifacts_bucket_key_id = ARTIFACTS_BUCKET_KEY_ID
        self._api_invoke_role_standard = API_INVOKE_ROLE_STANDARD

        self._event = event
        
        # amc
        self._amc_region = self._event["amcRegion"]
        self._amc_api_endpoint = self._event["amcApiEndpoint"]
        self._bucket_name = self._event["BucketName"]
        self._orange_room_account = self._event["amcOrangeAwsAccount"]
        self._team_name = self._event["amcTeamName"]
        self._red_room_account = self._event["amcRedAwsAccount"]
        self._amc_api_id = self.get_api_id_from_url()

        # s3 bucket
        self._bucket_account = self._event["bucketAccount"]
        self._bucket_region = self._event["bucketRegion"]
        self._bucket_exists = self._event["bucketExists"]

        # sns
        self._create_sns_topic = self._event["createSnsTopic"]
        self._sns_kms_key_id = SNS_KMS_KEY_ID

        # cloudformation
        self.stack_name = '{}-tps-instance-{}'.format(self._resource_prefix , self._event['TenantName'])
        
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
                    'ParameterKey': 'pInstanceApiEndpoint',
                    'ParameterValue': self._amc_api_endpoint
                },
                {
                    'ParameterKey': 'pWfmLambdaRoleNames',
                    'ParameterValue': self._wfm_lambda_roles_names
                },
                {
                    'ParameterKey': 'pRedRoomAccountId',
                    'ParameterValue': self._red_room_account
                },
                {
                    'ParameterKey': 'pInstanceRegion',
                    'ParameterValue': self._amc_region
                },
                {
                    'ParameterKey': 'pLoggingBucketName',
                    'ParameterValue': self._logging_bucket_name
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
                    'ParameterKey': 'pApiInvokeRoleStandard',
                    'ParameterValue': self._api_invoke_role_standard
                },
                {
                    'ParameterKey': 'pAmcApiId',
                    'ParameterValue': self._amc_api_id
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

    # methods to extract customer parameters
    def get_api_id_from_url(self):
        pattern = r'https:\/\/([a-z0-9]+)\.execute-api\.[a-z0-9\-]+\.amazonaws\.com\/.*'
        match = re.search(pattern, self._amc_api_endpoint)
        api_id = match.group(1)

        return api_id

    # methods to check customer configuration pattern
    def check_data_lake(self):
        if self._data_lake_enabled == "Yes":
            return True
        else:
            return False
            
    def check_cross_account(self):
        if self._bucket_account != self._application_account:
            return True 
        else:
            return False

    def check_cross_region(self):
        if self._bucket_region != self._application_region:
            return True
        else:
            return False

    def check_sns_create(self):
        if self._create_sns_topic == "true":
            return True 
        else:
            return False
        
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
            logger.info('Stack exists. Updating stack')
            return cfn.update_stack(
                                            StackName=stack_name,
                                            TemplateURL=template_url,
                                            Parameters=parameters,
                                            Capabilities=["CAPABILITY_NAMED_IAM","CAPABILITY_IAM"]
                                        )
        except Exception as e:
            if ('No updates are to be performed' in str(e)):
                return {"StackId" : stack_resp_desc["Stacks"][0]["StackId"] }
            else:
                logger.info('Stack does not exist. Creating stack')
                logger.info(f'Template_params: {parameters}')
                return cfn.create_stack(
                                                StackName=stack_name,
                                                TemplateURL=template_url,
                                                Parameters=parameters,
                                                Capabilities=["CAPABILITY_NAMED_IAM","CAPABILITY_IAM"]
                                        )
            
    def create_cross_account_data_lake_template(self):
        logger.info("Creating Cfn template for cross-account data lake")
        cross_account_data_lake_role_arn = f"arn:aws:iam::{self._application_account}:role/{self.stage_a_role_name}"
        cross_account_event_bridge_target = f"arn:aws:events:{self._application_region}:{self._application_account}:event-bus/default"

        return CrossAccountDataLakeTemplate(customer_id = self._event['TenantName'],
                                    bucket_name = self._event['BucketName'],
                                    cross_account_data_lake_role_arn = cross_account_data_lake_role_arn,
                                    cross_account_event_bridge_target = cross_account_event_bridge_target,
                                    orange_room_account_id = self._orange_room_account)  

    def create_cross_account_wfm_template(self):
        logger.info("Creating Cfn template for cross-account microservices")
        cross_account_api_role_name = f"{self._resource_prefix}-{self._application_region}-{self._event['TenantName']}-invokeAmcApiRole"

        return CrossAccountWfmTemplate(customer_id = self._event['TenantName'],
                                       cross_account_api_role_name = cross_account_api_role_name,
                                       application_account_id = self._application_account,
                                       amc_api_id=self._amc_api_id,
                                       amc_region=self._amc_region)
    
