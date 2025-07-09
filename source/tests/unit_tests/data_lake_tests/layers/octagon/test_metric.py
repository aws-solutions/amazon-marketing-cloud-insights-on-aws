# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for data_lake/layers/data_lake_library/python/datalake_library/octagon/metric
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/layers/octagon/test_metric.py


import os
import boto3
from unittest.mock import MagicMock
from moto import mock_aws

from data_lake.lambda_layers.data_lake_library.python.datalake_library.octagon.metric import MetricRecordInfo, MetricAPI


def test_metric_record_info():
    metric_record_info_cls = MetricRecordInfo(root="/", metric="test_metric", metric_type="test_type")
    assert metric_record_info_cls.root == "/"
    assert metric_record_info_cls.metric == "test_metric"
    assert metric_record_info_cls.metric_type == "test_type"

    assert f"[MRI root: {metric_record_info_cls.root}, metric: {metric_record_info_cls.metric}, metric_type: {metric_record_info_cls.metric_type}" == metric_record_info_cls.__str__()


def test_metric_api_create():
    for is_sns_set in [True, False]:
        sns_topic_name = "test_sns_topic"
        for test_expression in [["<", 10, 3], ["<=", 5, 3], ["=", 5, 5]]:
            with mock_aws():
                dynamodb_client = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
                table_name = "metric_table"
                notification_frequency = "ALWAYS"
                params = {
                    "TableName": table_name,
                    "KeySchema": [
                        {"AttributeName": "root", "KeyType": "HASH"},
                    ],
                    "AttributeDefinitions": [
                        {"AttributeName": "root", "AttributeType": "S"},
                    ],
                    "BillingMode": "PAY_PER_REQUEST",
                }
                dynamodb_client.create_table(**params)

                sns_client = boto3.client('sns')
                sns_client.create_topic(
                    Name=sns_topic_name,
                )
                build_test_metric = []
                for build_metric_test in ["Metric1", "Metric1#Metric2"]:
                    build_test_metric.append(MagicMock(
                        evaluation=test_expression[0],
                        threshold=test_expression[1],
                        notify=notification_frequency,
                        sns_topic=sns_topic_name,
                        metric_type="ROOT",
                        metric=build_metric_test
                    ))

                mock_config = MagicMock(metric_info=build_test_metric)
                mock_config.get_metrics_table.return_value = table_name
                mock_config.get_metrics_ttl.return_value = 1
                mock_client = MagicMock(
                    dynamodb=dynamodb_client,
                    config=mock_config,
                    pipeline_execution_id="1234567890",
                    sns_topic=sns_topic_name,
                    sns=sns_client,
                    region=os.environ["AWS_DEFAULT_REGION"],
                    account_id=os.environ["MOTO_ACCOUNT_ID"],
                )
                mock_client.is_pipeline_set.return_value = True
                mock_client.is_sns_set.return_value = is_sns_set

                metric_api_cls = MetricAPI(client=mock_client)

                assert metric_api_cls.logger is not None
                assert metric_api_cls.client == mock_client
                assert metric_api_cls.metrics_table == mock_client.dynamodb.Table(
                    mock_client.config.get_metrics_table())
                assert metric_api_cls.metrics_ttl == mock_client.config.get_metrics_ttl()

                assert None == metric_api_cls.create_metrics(date_str="2023-04-15", metric_code="test_metric", value=0)


@mock_aws
def test_metric_api_update():

    sns_client = boto3.client('sns')

    for i, test_expression in enumerate([[">", 3, 10, "ONCE"], [">=", 3, 5, "ALWAYS"]]):
        sns_topic_name = f"test_sns_topic_{test_expression[3]}"
        test_alt_sns_topic_name = ":".join(["arn", "aws", "sns", os.environ["AWS_DEFAULT_REGION"], os.environ["MOTO_ACCOUNT_ID"], sns_topic_name])
        
        with mock_aws():
            dynamodb_client = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
            table_name = f"metric_table_{i}"
            params = {
                "TableName": table_name,
                "KeySchema": [
                    {"AttributeName": "root", "KeyType": "HASH"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "root", "AttributeType": "S"},
                ],
                "BillingMode": "PAY_PER_REQUEST",
            }
            table = dynamodb_client.create_table(**params)

            sns_client.create_topic(
                Name=sns_topic_name,
            )

            build_test_metric = []
            for build_metric_test in ["Metric1", "Metric1#Metric2"]:
                build_test_metric.append(MagicMock(
                    evaluation=test_expression[0],
                    threshold=test_expression[1],
                    notify=test_expression[3],
                    sns_topic=test_alt_sns_topic_name,
                    metric_type="ROOT",
                    metric=build_metric_test
                ))

            mock_config = MagicMock(metric_info=build_test_metric)
            mock_config.get_metrics_table.return_value = table_name
            mock_config.get_metrics_ttl.return_value = 1
            mock_client = MagicMock(
                dynamodb=dynamodb_client,
                config=mock_config,
                pipeline_execution_id="1234567890",
                sns_topic=test_alt_sns_topic_name,
                sns=sns_client
            )
            mock_client.is_pipeline_set.return_value = True
            mock_client.is_sns_set.return_value = False

            metric_api_cls = MetricAPI(client=mock_client)

            assert metric_api_cls.logger is not None
            assert metric_api_cls.client == mock_client
            assert metric_api_cls.metrics_table == mock_client.dynamodb.Table(mock_client.config.get_metrics_table())
            assert metric_api_cls.metrics_ttl == mock_client.config.get_metrics_ttl()

    assert len(sns_client.list_topics()["Topics"]) == 2 # check if topics published
    assert sns_client.list_topics()["Topics"][0]["TopicArn"] == "arn:aws:sns:us-east-1:111111111111:test_sns_topic_ONCE"
    assert sns_client.list_topics()["Topics"][1]["TopicArn"] == "arn:aws:sns:us-east-1:111111111111:test_sns_topic_ALWAYS"


@mock_aws
def test_metric_api_no_notif():
    for test_expression in [[">", 3, 10, "ONCE"]]:
        sns_topic_name = "test_sns_topic"
        test_alt_sns_topic_name = ":".join(
            ["arn", "aws", "sns", os.environ["AWS_DEFAULT_REGION"], os.environ["MOTO_ACCOUNT_ID"], sns_topic_name])
        with mock_aws():
            dynamodb_client = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
            table_name = "metric_table"
            params = {
                "TableName": table_name,
                "KeySchema": [
                    {"AttributeName": "root", "KeyType": "HASH"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "root", "AttributeType": "S"},
                ],
                "BillingMode": "PAY_PER_REQUEST",
            }
            table = dynamodb_client.create_table(**params)
            sns_client = boto3.client('sns')
            sns_client.create_topic(
                Name=sns_topic_name,
            )

            build_test_metric = []
            for build_metric_test in ["Metric1"]:
                build_test_metric.append(MagicMock(
                    evaluation=test_expression[0],
                    threshold=test_expression[1],
                    notify=test_expression[3],
                    sns_topic=test_alt_sns_topic_name,
                    metric_type="ROOT",
                    metric=build_metric_test
                ))

            mock_config = MagicMock(metric_info=build_test_metric)
            mock_config.get_metrics_table.return_value = table_name
            mock_config.get_metrics_ttl.return_value = 1
            mock_client = MagicMock(
                dynamodb=dynamodb_client,
                config=mock_config,
                pipeline_execution_id="1234567890",
                sns_topic=test_alt_sns_topic_name,
                sns=sns_client
            )
            mock_client.is_pipeline_set.return_value = True
            mock_client.is_sns_set.return_value = False

            metric_api_cls = MetricAPI(client=mock_client)

            assert metric_api_cls.logger is not None
            assert metric_api_cls.client == mock_client
            assert metric_api_cls.metrics_table == mock_client.dynamodb.Table(mock_client.config.get_metrics_table())
            assert metric_api_cls.metrics_ttl == mock_client.config.get_metrics_ttl()

    assert len(sns_client.list_topics()["Topics"]) == 1  # check if topics published
    assert sns_client.list_topics()["Topics"][0]["TopicArn"] == "arn:aws:sns:us-east-1:111111111111:test_sns_topic"
