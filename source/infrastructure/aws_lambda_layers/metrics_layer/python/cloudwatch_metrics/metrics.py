# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_solutions.core.helpers import get_service_client


class Metrics:
    def __init__(self, metrics_namespace, resource_prefix, logger):
        """
        Initialize the Metrics class.

        :param metrics_namespace: The namespace for CloudWatch metrics.
        :param resource_prefix: A prefix used for CloudWatch metric dimensions.
        :param logger: Logger instance for logging information and errors.
        """
        self.metrics_namespace = metrics_namespace
        self.resource_prefix = resource_prefix
        self.logger = logger

    def put_metrics_count_value_1(self, metric_name):
        """
        Record a metric with a value of 1 in CloudWatch.

        :param metric_name: The name of the metric to record.
        """
        try:
            self.logger.info(
                f"Recording 1 (count) for metric {metric_name} in CloudWatch namespace {self.metrics_namespace}")
            cloudwatch_client = get_service_client('cloudwatch')

            cloudwatch_client.put_metric_data(
                Namespace=self.metrics_namespace,
                MetricData=[
                    {
                        'MetricName': metric_name,
                        'Dimensions': [{'Name': 'stack-name', 'Value': self.resource_prefix}],
                        'Value': 1,
                        'Unit': 'Count'
                    }
                ]
            )
        except Exception as e:
            # Log error but do not raise so that execution is not interrupted
            self.logger.error(f"Error recording metric {metric_name}: {e}")

    def put_nested_metrics(self, metric_name, nested_data):
        """
        Record nested metrics where `nested_data` is a dictionary of sub-metrics.

        :param metric_name: The base name of the metric to record.
        :param nested_data: A dictionary where keys are sub-metric names and values are counts to record.
        """
        try:
            self.logger.info(
                f"Recording nested metrics {nested_data} for {metric_name} in CloudWatch namespace {self.metrics_namespace}")

            metric_data = []
            for sub_metric, value in nested_data.items():
                metric_data.append({
                    'MetricName': f"{metric_name}_{sub_metric}",  # Construct metric name based on sub-metric
                    'Dimensions': [{'Name': 'stack-name', 'Value': self.resource_prefix}],
                    'Value': value,
                    'Unit': 'Count'
                })

            if metric_data:
                cloudwatch_client = get_service_client('cloudwatch')
                cloudwatch_client.put_metric_data(
                    Namespace=self.metrics_namespace,
                    MetricData=metric_data
                )
        except Exception as e:
            # Log error but do not raise so that execution is not interrupted
            self.logger.error(f"Error recording nested metrics for {metric_name}: {e}")
            
