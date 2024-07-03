# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for MicroServices/AMCInstancePostDeployMetadata Tps handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_tps_AMCInstancePostDeployMetadata_handler.py
###############################################################################


import os
import sys
import boto3
import pytest
from moto import mock_aws
from unittest.mock import patch, MagicMock

@pytest.fixture()
def _mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics

@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['DATA_LAKE_ENABLED'] = "Yes"
    os.environ['WFM_CUSTOMER_CONFIG_TABLE'] = "wfm_config_table"
    os.environ['TPS_CUSTOMER_CONFIG_TABLE'] = "tps_config_table"
    os.environ['SDLF_CUSTOMER_CONFIG_LOGICAL_ID'] = "sdlfConfigTable1234"
    os.environ['RESOURCE_PREFIX'] = "prefix"
    os.environ['AWS_ACCOUNT_ID'] = "123456789"
    os.environ['APPLICATION_REGION'] = "us-east-1"
    os.environ['API_INVOKE_ROLE_STANDARD'] = "amcinsights-us-east-1-123456789-invokeAmcApiRole"
    os.environ['CLOUD_TRAIL_ARN'] = 'arn:aws:cloudtrail:us-east-1:111111111111:trail/test-trail'
    os.environ['LOGGING_BUCKET_NAME'] = "testlogsbucket"


@mock_aws
@patch('aws_solutions.extended.resource_lookup.ResourceLookup.get_physical_id')
def test_handler(mock_get_physical_id, _mock_imports):
    from amc_insights.microservices.tenant_provisioning_service.lambdas.AMCInstancePostDeployMetadata.handler import handler
    
    SDLF_CUSTOMER_CONFIG_TABLE = 'sdlf_config_table'
    mock_get_physical_id.return_value = SDLF_CUSTOMER_CONFIG_TABLE

    # SETUP MOTO RESOURCES
    dynamodb =  boto3.resource("dynamodb", region_name=os.environ["APPLICATION_REGION"])
    tps_params = {
        "TableName": os.environ['TPS_CUSTOMER_CONFIG_TABLE'],
        "KeySchema": [
            {"AttributeName": "customerId", "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "customerId", "AttributeType": "S"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }
    dynamodb.create_table(**tps_params)

    sdlf_params = {
        "TableName": "sdlf_config_table",
        "KeySchema": [
            {"AttributeName": "customer_hash_key", "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "customer_hash_key", "AttributeType": "S"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }
    dynamodb.create_table(**sdlf_params)

    wfm_params = {
        "TableName": os.environ['WFM_CUSTOMER_CONFIG_TABLE'],
        "KeySchema": [
            {"AttributeName": "customerId", "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "customerId", "AttributeType": "S"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }
    dynamodb.create_table(**wfm_params)

    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-trail-bucket')
    cloudtrail = boto3.client("cloudtrail", region_name=os.environ["APPLICATION_REGION"])
    cloudtrail.create_trail(
        Name='test-trail',
        S3BucketName='test-trail-bucket'
    )
    test_selectors = [
            {'Name': 'S3EventSelector',
            'FieldSelectors':[
                {'Field': 'eventCategory', 'Equals': ['Data']}, 
                {'Field': 'resources.ARN', 'StartsWith': [
                    'arn:aws:s3:::amcinsights-bucketsartifact123456789/', 
                    'arn:aws:s3:::amcinsights-foundationsrawbucket123456789/', 
                    'arn:aws:s3:::amcinsights-foundationsstagebucket123456789/', 
                    'arn:aws:s3:::amcinsights-foundationsathenabucket123456789/'
                    ]
                }, 
                {'Field': 'resources.type', 'Equals': ['AWS::S3::Object']}
                ]
            },
            {'Name': 'LambdaEventSelector', 
            'FieldSelectors': [
                {'Field': 'eventCategory', 'Equals': ['Data']}, 
                {'Field': 'resources.ARN', 'StartsWith': [
                    f'arn:aws:lambda:{os.environ["APPLICATION_REGION"]}:{os.environ["AWS_ACCOUNT_ID"]}:function:amcinsights*'
                    ]
                }, 
                {'Field': 'resources.type', 'Equals': ['AWS::Lambda::Function']}
                ]
            }
        ]
    cloudtrail.put_event_selectors(
        TrailName=os.environ['CLOUD_TRAIL_ARN'],
        AdvancedEventSelectors=test_selectors
    )

    # TESTS
    customer = "first_customer"

    test_event = {
        "body": {
            "stackStatus": "CREATE_COMPLETE"
        },
        "TenantName": customer,
        "customerName": customer,
        "amcOrangeAwsAccount": "978365123",
        "BucketName": "bucket_name",
        "amcDatasetName": "test",
        "amcTeamName": "test",
        "bucketExists": True,
        "bucketAccount":  os.environ['AWS_ACCOUNT_ID'],
        "bucketRegion": os.environ['APPLICATION_REGION'],
        "snsTopicArn": "some_topic",
        "amcInstanceId": "amc12345",
        "amcAmazonAdsAdvertiserId": "12345",
        "amcAmazonAdsMarketplaceId": "12345",
    }

    expected_tps = {
        'customerId': customer,
        'customerName': customer,
        'amcOrangeAwsAccount': '978365123',
        'BucketName': 'bucket_name',
        'amcDatasetName': 'test',
        'amcTeamName': 'test',
        'bucketExists': True,
        'bucketAccount': os.environ['AWS_ACCOUNT_ID'],
        'bucketRegion': os.environ['APPLICATION_REGION'],
        "amcInstanceId": "amc12345",
        "amcAmazonAdsAdvertiserId": "12345",
        "amcAmazonAdsMarketplaceId": "12345",
    }

    expected_sdlf = {
        'customer_hash_key': customer,
        'dataset': 'test',
        'team': 'test',
        'hash_key': 'bucket_name',
        'prefix': customer
    }

    expected_wfm = {
        'customerId': customer,
        'outputSNSTopicArn': 'some_topic',
        "amcInstanceId": "amc12345",
        "amcAmazonAdsAdvertiserId": "12345",
        "amcAmazonAdsMarketplaceId": "12345",
    }

    expected_selectors = test_selectors.copy()
    expected_selectors[0]['FieldSelectors'][1]['StartsWith'].append('arn:aws:s3:::bucket_name/')

    response_create = handler(test_event, None)
    assert response_create is not None

    tps_table = dynamodb.Table(os.environ['TPS_CUSTOMER_CONFIG_TABLE'])
    response_tps = tps_table.scan(ConsistentRead=True)
    assert response_tps["Items"] == [expected_tps]

    sdlf_table = dynamodb.Table(SDLF_CUSTOMER_CONFIG_TABLE)
    response_sdlf = sdlf_table.scan(ConsistentRead=True)
    assert response_sdlf["Items"] == [expected_sdlf]

    wfm_table = dynamodb.Table(os.environ['WFM_CUSTOMER_CONFIG_TABLE'])
    response_wfm = wfm_table.scan(ConsistentRead=True)
    assert response_wfm["Items"] == [expected_wfm]


    customer = "second_customer"
    test_event["body"]["stackStatus"] = "UPDATE_COMPLETE"
    test_event["customerName"] = customer
    test_event["TenantName"] = customer

    expected_sdlf["customer_hash_key"] = customer
    expected_sdlf["prefix"] = customer

    expected_tps["customerName"] = customer
    expected_tps["customerId"] = customer

    expected_wfm["customerId"] = customer
    

    response_update = handler(test_event, None)
    assert response_update is not None
    
    response_tps = tps_table.scan(ConsistentRead=True)

    assert len(response_tps["Items"]) == 2

    assert response_tps["Items"][1] == expected_tps

    response_sdlf = sdlf_table.scan(ConsistentRead=True)
    assert len(response_sdlf["Items"]) == 2
    assert response_sdlf["Items"][1] == expected_sdlf

    response_wfm = wfm_table.scan(ConsistentRead=True)
    assert len(response_wfm["Items"]) == 2
    assert response_wfm["Items"][1] == expected_wfm

    with patch("amc_insights.microservices.tenant_provisioning_service.lambdas.AMCInstancePostDeployMetadata.handler.DATA_LAKE_ENABLED", "No"):
        customer = "3_customer"
        test_event["customerName"] = customer
        test_event["TenantName"] = customer

        expected_sdlf["customer_hash_key"] = customer
        expected_sdlf["prefix"] = customer

        expected_tps["customerName"] = customer
        expected_tps["customerId"] = customer

        expected_wfm["customerId"] = customer
        

        response_update = handler(test_event, None)
        assert response_update is not None
        
        response_tps = tps_table.scan(ConsistentRead=True)

        assert len(response_tps["Items"]) == 3

        assert expected_tps in response_tps["Items"]

        response_sdlf = sdlf_table.scan(ConsistentRead=True)
        assert len(response_sdlf["Items"]) == 2
        assert response_sdlf["Items"][1] != expected_sdlf

        response_wfm = wfm_table.scan(ConsistentRead=True)
        assert len(response_wfm["Items"]) == 3
        assert expected_wfm in response_wfm["Items"]

    test_event = {
        "body": {
            "stackStatus": "UNKNOWN"
        },
    }
    assert "Skipping Metadata Update in DDB" == handler(test_event, None)

    with pytest.raises(Exception):
        handler({}, None)