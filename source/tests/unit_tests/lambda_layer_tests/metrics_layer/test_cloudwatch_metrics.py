# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for infrastructure/aws_lambda_layers/metrics_layer/python/metrics/cloudwatch_metrics.
# USAGE:
#   ./run-unit-tests.sh --test-file-name lambda_layer_tests/metrics_layer/test_cloudwatch_metrics.py
###############################################################################
from aws_solutions.core.helpers import get_service_client
import logging
from moto import mock_aws
import pytest
from aws_lambda_layers.metrics_layer.python.cloudwatch_metrics.metrics import Metrics
logger = logging.getLogger()
cw_client = get_service_client("cloudwatch")

@mock_aws
@pytest.mark.run(order=1)
def test_put_metrics_count_value_1():
    metrics_namespace = "test3"
    resource_prefix = "test3"
    metrics_cls = Metrics(metrics_namespace=metrics_namespace, resource_prefix=resource_prefix, logger=logger)
    assert metrics_namespace == metrics_cls.metrics_namespace
    assert resource_prefix == metrics_cls.resource_prefix
    assert logger == metrics_cls.logger

    metric_name = "test_metric"
    expected_dimension = [{'Name': 'stack-name-2', 'Value': resource_prefix}]
    metrics_cls.put_metrics_count_value_1(metric_name=metric_name)

    resp = cw_client.list_metrics(
        Namespace=metrics_namespace,
        MetricName=metric_name,
        Dimensions=expected_dimension
    )
    assert resp is not None

@mock_aws
@pytest.mark.run(order=2)
def test_put_nested_metrics():
    metrics_namespace = "test2"
    resource_prefix = "test2"
    metrics_cls = Metrics(metrics_namespace=metrics_namespace, resource_prefix=resource_prefix, logger=logger)
    assert metrics_namespace == metrics_cls.metrics_namespace
    assert resource_prefix == metrics_cls.resource_prefix
    assert logger == metrics_cls.logger

    metric_name = "test_metric2"
    expected_dimension = [{'Name': 'stack-name-2', 'Value': resource_prefix}]
    nested_data= {"A": 1,  "B": 1}
    metrics_cls.put_nested_metrics(metric_name=metric_name, nested_data=nested_data)
    for key in nested_data.keys():
        resp = cw_client.list_metrics(
            Namespace=metrics_namespace,
            MetricName=f"{metric_name}_{key}",
            Dimensions=expected_dimension
        )
        assert resp is not None