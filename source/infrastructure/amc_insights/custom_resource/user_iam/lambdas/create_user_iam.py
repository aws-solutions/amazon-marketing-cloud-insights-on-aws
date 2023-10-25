# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
from crhelper import CfnResource
from aws_lambda_powertools import Logger
from aws_solutions.core.helpers import get_service_client

logger = Logger(service='Sync user roles to artifacts bucket', level="INFO")
helper = CfnResource()

STACK_NAME = os.environ['STACK_NAME']
APPLICATION_REGION = os.environ['APPLICATION_REGION']
APPLICATION_ACCOUNT = os.environ['APPLICATION_ACCOUNT']
SAGEMAKER_NOTEBOOK = os.environ['SAGEMAKER_NOTEBOOK']
SAGEMAKER_NOTEBOOK_LC = os.environ['SAGEMAKER_NOTEBOOK_LC']
TPS_INITIALIZE_SM_NAME = os.environ['TPS_INITIALIZE_SM_NAME']
WFM_WORKFLOWS_SM_NAME = os.environ['WFM_WORKFLOWS_SM_NAME']
WFM_WORKFLOW_EXECUTION_SM_NAME = os.environ['WFM_WORKFLOW_EXECUTION_SM_NAME']
STAGE_A_TRANSFORM_SM_NAME = os.environ['STAGE_A_TRANSFORM_SM_NAME']
STAGE_B_TRANSFORM_SM_NAME = os.environ['STAGE_B_TRANSFORM_SM_NAME']
DATALAKE_CUSTOMER_TABLE = os.environ['DATALAKE_CUSTOMER_TABLE']
WFM_CUSTOMER_TABLE = os.environ['WFM_CUSTOMER_TABLE']
WFM_WORKFLOWS_TABLE = os.environ['WFM_WORKFLOWS_TABLE']
WFM_WORKFLOW_EXECUTION_TABLE = os.environ['WFM_WORKFLOW_EXECUTION_TABLE']
TPS_CUSTOMER_TABLE = os.environ['TPS_CUSTOMER_TABLE']
OCTAGON_DATASETS_TABLE = os.environ['OCTAGON_DATASETS_TABLE']
OCTAGON_OBJECT_METADATA_TABLE = os.environ['OCTAGON_OBJECT_METADATA_TABLE']
OCTAGON_PIPELINE_EXECUTION_TABLE = os.environ['OCTAGON_PIPELINE_EXECUTION_TABLE']
OCTAGON_PIPELINE_TABLE = os.environ['OCTAON_PIPELINE_TABLE']
DATALAKE_CUSTOMER_TABLE_KEY = os.environ['DATALAKE_CUSTOMER_TABLE_KEY']
WFM_TABLE_KEY = os.environ['WFM_TABLE_KEY']
TPS_TABLE_KEY = os.environ['TPS_TABLE_KEY']
ARTIFACTS_BUCKET = os.environ['ARTIFACTS_BUCKET']
ARTIFACTS_BUCKET_KEY = os.environ['ARTIFACTS_BUCKET_KEY']
LOGGING_BUCKET = os.environ['LOGGING_BUCKET']
LOGGING_BUCKET_KEY = os.environ['LOGGING_BUCKET_KEY']
RAW_BUCKET = os.environ['RAW_BUCKET']
RAW_BUCKET_KEY = os.environ['RAW_BUCKET_KEY']
STAGE_BUCKET = os.environ['STAGE_BUCKET']
STAGE_BUCKET_KEY = os.environ['STAGE_BUCKET_KEY']
ATHENA_BUCKET = os.environ['ATHENA_BUCKET']
ATHENA_BUCKET_KEY = os.environ['ATHENA_BUCKET_KEY']
LAKE_FORMATION_CATALOG = os.environ['LAKE_FORMATION_CATALOG']
OCTAGON_DATASETS_TABLE_KEY = os.environ['OCTAGON_DATASETS_TABLE_KEY']
OCTAGON_OBJECT_METADATA_TABLE_KEY = os.environ['OCTAGON_OBJECT_METADATA_TABLE_KEY']
OCTAGON_PIPELINE_EXECUTION_TABLE_KEY = os.environ['OCTAGON_PIPELINE_EXECUTION_TABLE_KEY']
OCTAGON_PIPELINES_TABLE_KEY = os.environ['OCTAGON_PIPELINES_TABLE_KEY']
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']

FILE_NAME = 'IAM_POLICY_OPERATE.json'
IAM_POLICY_TEMPLATE = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:SearchTables",
                "glue:Get*",
                "athena:ListNamedQueries",
                "athena:GetWorkGroup",
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:ListQueryExecutions",
                "states:ListStateMachines",
                "states:DescribeStateMachine",
                "logs:DescribeLogGroups",
                "lambda:ListFunctions",
                "dynamodb:ListTables",
                "dynamodb:DescribeTable",
                "iam:ListRoles",
                "iam:ListUsers",
                "lambda:GetAccountSettings",
                "events:Describe*",
                "s3:ListAllMyBuckets",
                "events:List*",
                "sagemaker:ListNotebookInstances"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sagemaker:*"
            ],
            "Resource": [
                SAGEMAKER_NOTEBOOK,
                SAGEMAKER_NOTEBOOK_LC
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:*"
            ],
            "Resource": [
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{TPS_INITIALIZE_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{TPS_INITIALIZE_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{WFM_WORKFLOWS_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{WFM_WORKFLOWS_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{WFM_WORKFLOW_EXECUTION_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{WFM_WORKFLOW_EXECUTION_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{STAGE_A_TRANSFORM_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{STAGE_A_TRANSFORM_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{STAGE_B_TRANSFORM_SM_NAME}*",
                f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{STAGE_B_TRANSFORM_SM_NAME}*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": [
                RAW_BUCKET,
                f"{RAW_BUCKET}/*",
                STAGE_BUCKET,
                f"{STAGE_BUCKET}/*",
                ATHENA_BUCKET,
                f"{ATHENA_BUCKET}/*",
                LOGGING_BUCKET,
                f"{LOGGING_BUCKET}/*",
                ARTIFACTS_BUCKET,
                f"{ARTIFACTS_BUCKET}/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:*"
            ],
            "Resource": [
                f"{TPS_CUSTOMER_TABLE}*",
                f"{WFM_CUSTOMER_TABLE}*",
                f"{WFM_WORKFLOWS_TABLE}*",
                f"{WFM_WORKFLOW_EXECUTION_TABLE}*",
                f"{DATALAKE_CUSTOMER_TABLE}*",
                f"{OCTAGON_DATASETS_TABLE}*",
                f"{OCTAGON_OBJECT_METADATA_TABLE}*",
                f"{OCTAGON_PIPELINE_EXECUTION_TABLE}*",
                f"{OCTAGON_PIPELINE_TABLE}*",
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kms:*"
            ],
            "Resource": [
                TPS_TABLE_KEY,
                WFM_TABLE_KEY,
                DATALAKE_CUSTOMER_TABLE_KEY,
                RAW_BUCKET_KEY,
                STAGE_BUCKET_KEY,
                ATHENA_BUCKET_KEY,
                LOGGING_BUCKET_KEY,
                ARTIFACTS_BUCKET_KEY,
                OCTAGON_DATASETS_TABLE_KEY,
                OCTAGON_OBJECT_METADATA_TABLE_KEY,
                OCTAGON_PIPELINE_EXECUTION_TABLE_KEY,
                OCTAGON_PIPELINES_TABLE_KEY
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "events:*"
            ],
            "Resource": [
                f"arn:aws:events:*:{APPLICATION_ACCOUNT}:rule/amc*",
                f"arn:aws:events:*:{APPLICATION_ACCOUNT}:rule/{STACK_NAME}*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:*"
            ],
            "Resource": [
                f"arn:aws:lambda:*:{APPLICATION_ACCOUNT}:function:{STACK_NAME}*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:*"
            ],
            "Resource": [
                f"arn:aws:logs:*:{APPLICATION_ACCOUNT}:log-group:/aws/*/{STACK_NAME}*:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:*"
            ],
            "Resource": [
                LAKE_FORMATION_CATALOG
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudformation:*"
            ],
            "Resource": [
                f"arn:aws:cloudformation:*:{APPLICATION_ACCOUNT}:stack/{STACK_NAME}*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:*"
            ],
            "Resource": [
                f"arn:aws:glue:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:job/{GLUE_JOB_NAME}"
            ]
        }
    ]
}

def event_handler(event, context):
    """
    This function is the entry point for the Lambda-backed custom resource.
    """
    logger.info(event)
    helper(event, context)

def delete_bucket_object(resource_properties) -> None:
    s3_client = get_service_client("s3")
    artifacts_bucket_name, object_key = get_bucket_name_and_key(resource_properties, FILE_NAME)
    s3_client.delete_object(
        Bucket=artifacts_bucket_name,
        Key=object_key
    )
    logger.info(f"Deleted {object_key}")

def upload_bucket_object(resource_properties, file_object) -> None:
    s3_client = get_service_client("s3")
    artifacts_bucket_name, object_key = get_bucket_name_and_key(resource_properties, FILE_NAME)
    formatted_object = bytes(json.dumps(file_object).encode("UTF-8"))
    s3_client.put_object(
        Body=formatted_object,
        Bucket=artifacts_bucket_name,
        Key=object_key,
    )
    logger.info(f"Uploaded {object_key}")

def get_bucket_name_and_key(resource_properties, file):
    artifacts_bucket_name: str = resource_properties["artifacts_bucket_name"]
    artifacts_key_prefix: str = resource_properties["artifacts_key_prefix"]
    object_key = f"{artifacts_key_prefix}{file}"
    return artifacts_bucket_name, object_key

@helper.create
@helper.update
def create_update(event, _):
    logger.info(f"Create event input: {json.dumps(event)}")
    resource_properties = event["ResourceProperties"]
    try:
        upload_bucket_object(resource_properties=resource_properties, file_object=IAM_POLICY_TEMPLATE)
    except Exception as err:
        logger.error(err)
        raise err
    logger.info(
        f'Move IAM Role Operate file to S3 artifacts {resource_properties["artifacts_bucket_name"]} {resource_properties["artifacts_key_prefix"]}')

@helper.delete
def on_delete(event, _):
    logger.info(f"Create event input: {json.dumps(event)}")
    resource_properties = event["ResourceProperties"]
    try:
        delete_bucket_object(resource_properties)
    except Exception as err:
        logger.error(err)
        raise err
    logger.info("IAM Role Operate file deleted.")

