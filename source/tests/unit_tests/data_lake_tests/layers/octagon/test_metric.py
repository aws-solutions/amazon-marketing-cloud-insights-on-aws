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
from datetime import datetime
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

                assert True == metric_api_cls.create_metrics(date_str="2023-06-30", metric_code="Metric1#Metric2",
                                                             value=test_expression[2])

                sns_topic_arn = ":".join(
                    ["arn", "aws", "sns", os.environ["AWS_DEFAULT_REGION"], os.environ["MOTO_ACCOUNT_ID"],
                     sns_topic_name])
                for metric_test in ["Metric1", "Metric1#Metric2"]:
                    table = dynamodb_client.Table(table_name)
                    result = table.get_item(
                        Key={"root": metric_test, "metric": metric_test}, ConsistentRead=True,
                        AttributesToGet=["root", "metric", "version"]
                    )

                    assert result["Item"] is not None
                    assert result["Item"]["root"] == metric_test
                    assert result["Item"]["metric"] == metric_test
                    assert result["Item"]["notification_frequency"] == notification_frequency
                    assert int(result["Item"]["notification_threshold"]) == test_expression[1]
                    assert result["Item"]["notification_sns_topic_arn"] == sns_topic_arn

                    for expected_item in [
                        "creation_timestamp", "last_updated_timestamp",
                        "last_updated_date", "last_pipeline_execution_id", "value",
                        "ttl", "notification_sns_message_id", "version"
                    ]:
                        assert result["Item"][expected_item] is not None

                assert len(sns_client.list_topics()["Topics"]) == 1

                assert sns_client.list_topics()["Topics"][0]["TopicArn"] == sns_topic_arn


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

            
            items = [
                    {
                    "version": 2,
                    "metric": "Metric1",
                    "Value": 3,
                    "root": "Metric1",
                },
                {
                    "version": 3,
                    "metric": "Metric1#Metric2",
                    "Value": 4,
                    "root": "Metric1#Metric2",
                },
            ]

            for item in items:
                table.put_item(
                    TableName=table_name, Item=item
                )

            assert True == metric_api_cls.create_metrics(date_str="2023-06-30", metric_code="Metric1#Metric2", value=test_expression[2])

            for metric_test in ["Metric1", "Metric1#Metric2"]:
                table = dynamodb_client.Table(table_name)
                result = table.get_item(
                    Key={"root": metric_test, "metric": metric_test}, ConsistentRead=True,
                    AttributesToGet=["root", "metric", "version"]
                )

                assert result["Item"] is not None
                assert result["Item"]["root"] == metric_test
                assert result["Item"]["metric"] == metric_test

                assert result["Item"]["notification_frequency"] == test_expression[3]
                assert int(result["Item"]["notification_threshold"]) == test_expression[1]
                assert result["Item"]["notification_sns_topic_arn"] == test_alt_sns_topic_name
                assert result["Item"]["notification_sns_message_id"] is not None # test_notification

                for expected_item in [
                        "last_updated_timestamp",
                        "last_updated_date", "last_pipeline_execution_id", "value",
                        "notification_sns_message_id"
                    ]:
                        assert result["Item"][expected_item] is not None

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

            notif_tmpstmp = {}
            if test_expression[3] == "ONCE":
                notif_tmpstmp = {"notification_timestamp": datetime.now().strftime("%Y-%m-%d")}

            items = [
                {
                    "version": 2,
                    "metric": "Metric1",
                    "Value": 3,
                    "root": "Metric1",
                    **notif_tmpstmp
                },
                {
                    "version": 3,
                    "metric": "Metric1#Metric2",
                    "Value": 4,
                    "root": "Metric1#Metric2",
                    **notif_tmpstmp
                },
            ]
            for item in items:
                table.put_item(
                    TableName=table_name, Item=item
                )

            assert True == metric_api_cls.create_metrics(date_str="2023-06-30", metric_code="Metric1#Metric2",
                                                         value=test_expression[2])

            for metric_test in ["Metric1", "Metric1#Metric2"]:
                table = dynamodb_client.Table(table_name)
                result = table.get_item(
                    Key={"root": metric_test, "metric": metric_test}, ConsistentRead=True,
                    AttributesToGet=["root", "metric", "version"]
                )

                assert result["Item"] is not None
                assert result["Item"]["root"] == metric_test
                assert result["Item"]["metric"] == metric_test
                assert result["Item"].get("notification_sns_message_id") is None

    assert len(sns_client.list_topics()["Topics"]) == 1  # check if topics published
    assert sns_client.list_topics()["Topics"][0]["TopicArn"] == "arn:aws:sns:us-east-1:111111111111:test_sns_topic"
