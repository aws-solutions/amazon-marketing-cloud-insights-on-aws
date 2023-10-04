# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for InvokeWorkflowExecutionSM/handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/layers/test_datalake_lib_event_config.py



import os
from urllib import parse

import pytest
from moto import mock_ssm

from data_lake.lambda_layers.data_lake_library.python.datalake_library.configuration.event_configs import EventConfig, EmptyEventConfig, S3EventConfig


@mock_ssm
def test_event_config():
    with pytest.raises(NotImplementedError):
        event_config_cls = EventConfig(event={})
        assert event_config_cls._event == {}
        assert event_config_cls._ssm_interface != None
        assert event_config_cls.log_level == os.getenv('LOG_LEVEL', 'INFO')
        assert hasattr(event_config_cls, "_logger")
        assert hasattr(event_config_cls, "_fetch_from_event")

    with pytest.raises(NotImplementedError):
        test_ssm_interface = "test_ssm_interface"
        event_config_cls = EventConfig(event={}, ssm_interface=test_ssm_interface)
        assert event_config_cls._event == {}
        assert event_config_cls._ssm_interface == test_ssm_interface


@mock_ssm
def test_empty_event_config():
    with pytest.raises(NotImplementedError):
        empty_event_config_cls = EmptyEventConfig()
        assert empty_event_config_cls._event == None
        assert empty_event_config_cls._ssm_interface != None
        assert empty_event_config_cls.log_level == os.getenv('LOG_LEVEL', 'INFO')
        assert hasattr(empty_event_config_cls, "_fetch_from_event")


@mock_ssm
def test_s3_event_config():
    test_event = {
        "Records": [
            {
                "awsRegion": os.environ["AWS_REGION"],
                "s3": {
                    "bucket": {
                        "name": "s3bucket-raw"
                    },
                    "object": {
                        "key": "bucket_folder/bucket_key",
                        "size": "9999"
                    }
                },
                "eventTime": "10/01/1960"
            }
        ]
    }
    s3_event_config_cls = S3EventConfig(event=test_event)
    assert s3_event_config_cls._event == test_event
    assert s3_event_config_cls._ssm_interface != None
    assert s3_event_config_cls.log_level == os.getenv('LOG_LEVEL', 'INFO')
    assert hasattr(s3_event_config_cls, "_fetch_from_event")
    assert s3_event_config_cls._size == int(test_event['Records'][0]['s3']['object']['size'])
    assert s3_event_config_cls._landing_time == test_event['Records'][0]['eventTime']

    ## test raw
    expected_key = parse.unquote_plus(test_event['Records'][0]['s3']['object']['key'].encode('utf8').decode('utf8'))
    assert s3_event_config_cls._region == test_event['Records'][0]['awsRegion']
    assert s3_event_config_cls._source_bucket == test_event['Records'][0]['s3']['bucket']['name']
    assert s3_event_config_cls._object_key == expected_key
    assert s3_event_config_cls._stage == test_event['Records'][0]['s3']['bucket']['name'].split('-')[-1]
    assert s3_event_config_cls._team == expected_key.split('/')[0]
    assert s3_event_config_cls._dataset == expected_key.split('/')[1]

    ## test stage
    test_event['Records'][0]['s3']['bucket']['name'] = "s3bucket-stage"
    s3_event_config_cls = S3EventConfig(event=test_event)
    assert s3_event_config_cls._stage == test_event['Records'][0]['s3']['bucket']['name'].split('-')[-1]

    ## test analytics
    test_event['Records'][0]['s3']['bucket']['name'] = "s3bucket-analytics"
    s3_event_config_cls = S3EventConfig(event=test_event)
    assert s3_event_config_cls._stage == test_event['Records'][0]['s3']['bucket']['name'].split('-')[-1]

    ## test no tag
    test_event['Records'][0]['s3']['bucket']['name'] = "s3bucket-None"
    expected_key = "bucket_folder/bucket_folderA/some_file"
    test_event['Records'][0]['s3']['object']['key'] = expected_key
    expected_key = parse.unquote_plus(expected_key)
    s3_event_config_cls = S3EventConfig(event=test_event)
    assert s3_event_config_cls._stage == expected_key.split('/')[0]
    assert s3_event_config_cls._team == expected_key.split('/')[1]
    assert s3_event_config_cls._dataset == expected_key.split('/')[2]

    with pytest.raises(ValueError) as ex:
        test_error_event = {
            "detail": {
                "errorCode": "error",
                "error_code": "error-error",
                "raw_s3_bucket": "test_bucket/s3folder-raw",
                "file_key": "bucket_key"
            }
        }
        S3EventConfig(event=test_error_event)

    msg = 'Event refers to a failed command: ' \
            'error_code {}, bucket {}, object {}'.format(test_error_event['detail']['error_code'],
                                                        test_error_event['detail']['raw_s3_bucket'],
                                                        test_error_event['detail']['file_key'])
    assert  msg in str(ex)

    ### test falling back on CloudTrail Event
    test_fallback_cloudtrail_event = {
        "detail": {
            "awsRegion": os.environ["AWS_REGION"],
            "requestParameters": {
                "bucketName": "s3bucket-raw",
                "key": "bucket_folder/bucket_key",
            },
            "additionalEventData": {
                "bytesTransferredIn": "1000"
            },
            "eventTime": "10011960",
        }
    }

    s3_event_config_cls = S3EventConfig(event=test_fallback_cloudtrail_event)
    expected_key = parse.unquote_plus(
        test_fallback_cloudtrail_event['detail']['requestParameters']['key'].encode('utf8').decode('utf8'))
    assert s3_event_config_cls._region == test_fallback_cloudtrail_event['detail']['awsRegion']
    assert s3_event_config_cls._source_bucket == test_fallback_cloudtrail_event['detail']['requestParameters']['bucketName']
    assert s3_event_config_cls._object_key == expected_key
    assert s3_event_config_cls._size == int(test_fallback_cloudtrail_event['detail']['additionalEventData']['bytesTransferredIn'])
    assert s3_event_config_cls._landing_time == test_fallback_cloudtrail_event['detail']['eventTime']

    
    ## test raw
    assert s3_event_config_cls._stage == test_fallback_cloudtrail_event['detail']['requestParameters']['bucketName'].split('-')[-1]
    assert s3_event_config_cls._team == expected_key.split('/')[0]
    assert s3_event_config_cls._dataset == expected_key.split('/')[1]

    ## test stage
    test_fallback_cloudtrail_event['detail']['requestParameters']['bucketName'] = "s3bucket-stage"
    s3_event_config_cls = S3EventConfig(event=test_fallback_cloudtrail_event)
    assert s3_event_config_cls._stage == test_fallback_cloudtrail_event['detail']['requestParameters']['bucketName'].split('-')[-1]

    ## test analytics
    test_fallback_cloudtrail_event['detail']['requestParameters']['bucketName'] = "s3bucket-analytics"
    s3_event_config_cls = S3EventConfig(event=test_fallback_cloudtrail_event)
    assert s3_event_config_cls._stage == test_fallback_cloudtrail_event['detail']['requestParameters']['bucketName'].split('-')[-1]

    ## test no tag
    test_fallback_cloudtrail_event['detail']['requestParameters']['bucketName'] = "s3bucket-None"
    expected_key = "bucket_folder/bucket_folderA/some_file"
    test_fallback_cloudtrail_event['detail']['requestParameters']['key'] = expected_key
    expected_key = parse.unquote_plus(expected_key)
    s3_event_config_cls = S3EventConfig(event=test_fallback_cloudtrail_event)
    assert s3_event_config_cls._stage == expected_key.split('/')[0]
    assert s3_event_config_cls._team == expected_key.split('/')[1]
    assert s3_event_config_cls._dataset == expected_key.split('/')[2]

    assert hasattr(s3_event_config_cls, "source_bucket")
    assert hasattr(s3_event_config_cls, "region")
    assert hasattr(s3_event_config_cls, "object_key")
    assert hasattr(s3_event_config_cls, "stage")
    assert hasattr(s3_event_config_cls, "dataset")
    assert hasattr(s3_event_config_cls, "size")
    assert hasattr(s3_event_config_cls, "landing_time")

    assert s3_event_config_cls.source_bucket == "s3bucket-None"
    assert s3_event_config_cls.region == os.environ["AWS_REGION"]
    assert s3_event_config_cls.object_key == expected_key
    assert s3_event_config_cls.stage == expected_key.split('/')[0]
    assert s3_event_config_cls.dataset == expected_key.split('/')[2]
    assert s3_event_config_cls.size == int(test_fallback_cloudtrail_event['detail']['additionalEventData']['bytesTransferredIn'])
    assert s3_event_config_cls.landing_time == test_fallback_cloudtrail_event['detail']['eventTime']

