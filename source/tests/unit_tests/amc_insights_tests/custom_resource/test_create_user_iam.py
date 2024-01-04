# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for custom_resource/user_iam/create_user_iam.py.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/custom_resource/test_create_user_iam.py


import os
import boto3
import pytest
import json
from moto import mock_s3
from unittest.mock import Mock, patch
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients


@pytest.fixture()
def _mock_s3_client():
    s3_client = get_service_client('s3')
    s3_client.upload_file = Mock(
        return_value={
        }
    )
    s3_client.delete_object = Mock(
        return_value={

        }
    )
    return s3_client


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_s3_client):
    monkeypatch.setitem(_helpers_service_clients, 's3', _mock_s3_client)



@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['STACK_NAME'] = "test_stack_name"
    os.environ['APPLICATION_ACCOUNT'] = os.environ["MOTO_ACCOUNT_ID"]
    os.environ['SAGEMAKER_NOTEBOOK'] = "test_sage_ntbk"
    os.environ['SAGEMAKER_NOTEBOOK_LC'] = "test_sage_ntbk_lc"
    os.environ['TPS_INITIALIZE_SM'] = "test_tps_init_sm"
    os.environ['WFM_WORKFLOWS_SM'] = "test_wfm_wfs_sm"
    os.environ['WFM_WORKFLOW_EXECUTION_SM'] = "test_wfm_wf_exe_sm"
    os.environ['DATALAKE_CUSTOMER_TABLE'] = "test_data_lake_customer_table"
    os.environ['WFM_CUSTOMER_TABLE'] = "test_wfm_cust_table"
    os.environ['WFM_WORKFLOWS_TABLE'] = "test_wfm_wf_table"
    os.environ['WFM_WORKFLOW_EXECUTION_TABLE'] = "test_wfm_wf_exe_table"
    os.environ['TPS_CUSTOMER_TABLE'] = "test_tps_cust_table"
    os.environ['OCTAGON_DATASETS_TABLE'] = "test_oct_dts_table"
    os.environ['OCTAGON_OBJECT_METADATA_TABLE'] = "test_oct_obj_meta_table"
    os.environ['OCTAGON_PIPELINE_EXECUTION_TABLE'] = "test_oct_pipeline_ex_table"
    os.environ['OCTAON_PIPELINE_TABLE'] = "test_oct_pipeline_table"
    os.environ['DATALAKE_CUSTOMER_TABLE_KEY'] = "test_dt_lake_cust_table_key"
    os.environ['WFM_TABLE_KEY'] = "test_wfm_table_key"
    os.environ['TPS_TABLE_KEY'] = "test_tps_table_key"
    os.environ['ARTIFACTS_BUCKET'] = "test_artifact_bucket"
    os.environ['ARTIFACTS_BUCKET_KEY'] = "test_artifact_bucket_key"
    os.environ['LOGGING_BUCKET'] = "test_logging_bucket"
    os.environ['LOGGING_BUCKET_KEY'] = "test_bucket_key"
    os.environ['RAW_BUCKET'] = "test_raw_bucket"
    os.environ['RAW_BUCKET_KEY'] = "test_raw_bucket_key"
    os.environ['STAGE_BUCKET'] = "test_stage_bucket"
    os.environ['STAGE_BUCKET_KEY'] = "test_stage_bucket_key"
    os.environ['ATHENA_BUCKET'] = "test_athena_bucket"
    os.environ['ATHENA_BUCKET_KEY'] = "test_athena_bucket_key"
    os.environ['LAKE_FORMATION_CATALOG'] = "test_lake_formation_catalog"
    os.environ['APPLICATION_REGION'] = "us-east-1"
    os.environ['TPS_INITIALIZE_SM_NAME'] = "test_sm_name"
    os.environ['WFM_WORKFLOWS_SM_NAME'] = "test_sm_name"
    os.environ['WFM_WORKFLOW_EXECUTION_SM_NAME'] = "test_sm_name"
    os.environ['STAGE_A_TRANSFORM_SM_NAME'] = "test_sm_name"
    os.environ['STAGE_B_TRANSFORM_SM_NAME'] = "test_sm_name"
    os.environ['OCTAGON_DATASETS_TABLE_KEY'] = "test_table_key"
    os.environ['OCTAGON_OBJECT_METADATA_TABLE_KEY'] = "test_table_key"
    os.environ['OCTAGON_PIPELINE_EXECUTION_TABLE_KEY'] = "test_table_key"
    os.environ['OCTAGON_PIPELINES_TABLE_KEY'] = "test_table_key"
    os.environ['GLUE_JOB_NAME'] = "test_glue_name"

def test_globals():
    from amc_insights.custom_resource.user_iam.lambdas.create_user_iam import (
        FILE_NAME, IAM_POLICY_TEMPLATE, STACK_NAME, APPLICATION_ACCOUNT,
        SAGEMAKER_NOTEBOOK, SAGEMAKER_NOTEBOOK_LC, DATALAKE_CUSTOMER_TABLE, WFM_CUSTOMER_TABLE, WFM_WORKFLOWS_TABLE,
        WFM_WORKFLOW_EXECUTION_TABLE, TPS_CUSTOMER_TABLE, OCTAGON_DATASETS_TABLE, OCTAGON_OBJECT_METADATA_TABLE,
        OCTAGON_PIPELINE_EXECUTION_TABLE, OCTAGON_PIPELINE_TABLE, DATALAKE_CUSTOMER_TABLE_KEY, DATALAKE_CUSTOMER_TABLE_KEY,
        WFM_TABLE_KEY, TPS_TABLE_KEY, ARTIFACTS_BUCKET, ARTIFACTS_BUCKET_KEY,LOGGING_BUCKET, LOGGING_BUCKET_KEY, RAW_BUCKET,
        RAW_BUCKET_KEY, STAGE_BUCKET, STAGE_BUCKET_KEY, ATHENA_BUCKET, ATHENA_BUCKET_KEY, LAKE_FORMATION_CATALOG,
        APPLICATION_REGION, TPS_INITIALIZE_SM_NAME, WFM_WORKFLOWS_SM_NAME, WFM_WORKFLOW_EXECUTION_SM_NAME, STAGE_A_TRANSFORM_SM_NAME, 
        STAGE_B_TRANSFORM_SM_NAME, OCTAGON_DATASETS_TABLE_KEY, OCTAGON_OBJECT_METADATA_TABLE_KEY, OCTAGON_PIPELINE_EXECUTION_TABLE_KEY,
        OCTAGON_PIPELINES_TABLE_KEY, GLUE_JOB_NAME
    )

    assert FILE_NAME == 'IAM_POLICY_OPERATE.json'
    assert STACK_NAME == os.environ['STACK_NAME']
    assert APPLICATION_ACCOUNT == os.environ['APPLICATION_ACCOUNT']
    assert SAGEMAKER_NOTEBOOK == os.environ['SAGEMAKER_NOTEBOOK']
    assert SAGEMAKER_NOTEBOOK_LC == os.environ['SAGEMAKER_NOTEBOOK_LC']
    assert DATALAKE_CUSTOMER_TABLE == os.environ['DATALAKE_CUSTOMER_TABLE']
    assert WFM_CUSTOMER_TABLE == os.environ['WFM_CUSTOMER_TABLE']
    assert WFM_WORKFLOWS_TABLE == os.environ['WFM_WORKFLOWS_TABLE']
    assert WFM_WORKFLOW_EXECUTION_TABLE == os.environ['WFM_WORKFLOW_EXECUTION_TABLE']
    assert TPS_CUSTOMER_TABLE == os.environ['TPS_CUSTOMER_TABLE']
    assert OCTAGON_DATASETS_TABLE == os.environ['OCTAGON_DATASETS_TABLE']
    assert OCTAGON_OBJECT_METADATA_TABLE == os.environ['OCTAGON_OBJECT_METADATA_TABLE']
    assert OCTAGON_PIPELINE_EXECUTION_TABLE == os.environ['OCTAGON_PIPELINE_EXECUTION_TABLE']
    assert OCTAGON_PIPELINE_TABLE == os.environ['OCTAON_PIPELINE_TABLE']
    assert DATALAKE_CUSTOMER_TABLE_KEY == os.environ['DATALAKE_CUSTOMER_TABLE_KEY']
    assert WFM_TABLE_KEY == os.environ['WFM_TABLE_KEY']
    assert TPS_TABLE_KEY == os.environ['TPS_TABLE_KEY']
    assert ARTIFACTS_BUCKET == os.environ['ARTIFACTS_BUCKET']
    assert ARTIFACTS_BUCKET_KEY == os.environ['ARTIFACTS_BUCKET_KEY']
    assert LOGGING_BUCKET == os.environ['LOGGING_BUCKET']
    assert LOGGING_BUCKET_KEY == os.environ['LOGGING_BUCKET_KEY']
    assert RAW_BUCKET == os.environ['RAW_BUCKET']
    assert RAW_BUCKET_KEY == os.environ['RAW_BUCKET_KEY']
    assert STAGE_BUCKET == os.environ['STAGE_BUCKET']
    assert STAGE_BUCKET_KEY == os.environ['STAGE_BUCKET_KEY']
    assert ATHENA_BUCKET == os.environ['ATHENA_BUCKET']
    assert ATHENA_BUCKET_KEY == os.environ['ATHENA_BUCKET_KEY']
    assert LAKE_FORMATION_CATALOG == os.environ['LAKE_FORMATION_CATALOG']

    expected_iam_policy_template = {
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

    assert IAM_POLICY_TEMPLATE == expected_iam_policy_template


@pytest.fixture
def lambda_event():
    return {
            "ResourceProperties":
                {
                    "artifacts_bucket_name": "artifacts_bucket",
                    "artifacts_key_prefix": "create_user_iam/",
                },
            "RequestType": "Update",
        }


@patch("amc_insights.custom_resource.user_iam.lambdas.create_user_iam.helper")
def test_event_handler(mock_helper):
    from amc_insights.custom_resource.user_iam.lambdas.create_user_iam import event_handler

    event_handler({}, None)

    mock_helper.assert_called_with({}, None)


def test_delete_bucket_object(_mock_clients, lambda_event):
    from amc_insights.custom_resource.user_iam.lambdas.create_user_iam import delete_bucket_object

    delete_bucket_object(resource_properties=lambda_event["ResourceProperties"])


@mock_s3
def test_upload_bucket_object(lambda_event):
    from amc_insights.custom_resource.user_iam.lambdas.create_user_iam import upload_bucket_object, FILE_NAME

    expected_file_object = {
        "test_file": "test_obj"
    }
    s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
    s3.create_bucket(Bucket=lambda_event["ResourceProperties"]["artifacts_bucket_name"])
    s3_resource = boto3.resource("s3")
    s3_resource.Object(
       lambda_event["ResourceProperties"]["artifacts_bucket_name"], f"{lambda_event['ResourceProperties']['artifacts_key_prefix']}{FILE_NAME}"
    )

    upload_bucket_object(resource_properties=lambda_event["ResourceProperties"], file_object=expected_file_object)

    source_s3_object = s3_resource.Object(
        lambda_event["ResourceProperties"]["artifacts_bucket_name"],
        f"{lambda_event['ResourceProperties']['artifacts_key_prefix']}{FILE_NAME}",
    ).get()

    assert json.loads(source_s3_object["Body"].read().decode('utf-8')) == expected_file_object


def test_get_bucket_name_and_key(lambda_event):
    from amc_insights.custom_resource.user_iam.lambdas.create_user_iam import get_bucket_name_and_key
 
    artifacts_bucket_name, object_key = get_bucket_name_and_key(
        resource_properties=lambda_event["ResourceProperties"],
        file="test"
    )
 
    assert artifacts_bucket_name == lambda_event["ResourceProperties"]["artifacts_bucket_name"]
    assert object_key == f"{lambda_event['ResourceProperties']['artifacts_key_prefix']}test"


def test_on_create_update(_mock_clients, lambda_event):
    with patch("amc_insights.custom_resource.user_iam.lambdas.create_user_iam.upload_bucket_object") as upload_bucket_object_mock:
        from amc_insights.custom_resource.user_iam.lambdas.create_user_iam import create_update
 
        create_update(lambda_event, None)
        upload_bucket_object_mock.assert_called_once()
 
    # cover exception
    with pytest.raises(Exception):
        create_update({"ResourceProperties": {}}, None)


def test_on_delete(_mock_clients, lambda_event):
    with patch("amc_insights.custom_resource.user_iam.lambdas.create_user_iam.delete_bucket_object") as delete_bucket_object_mock:
        from amc_insights.custom_resource.user_iam.lambdas.create_user_iam import on_delete
 
        on_delete(lambda_event, None)
        delete_bucket_object_mock.assert_called_once()
 
    # cover exception
    with pytest.raises(Exception):
        on_delete({"ResourceProperties": {}}, None)
    



