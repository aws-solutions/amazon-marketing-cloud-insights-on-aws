# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import datetime as dt

import boto3
from botocore.config import Config


class GlueUtilities:
    """
    A utility class for AWS Glue jobs, providing common functionality such as 
    logging setup, service client/resource creation, and recording S3 metrics for 
    bytes read and written during transformation jobs. This class is intended to 
    be reusable across multiple Glue scripts, promoting consistent logging and metrics reporting.
    """
    def __init__(self, solution_args: dict):
        """
        Initializes the GlueUtilities class by setting up a custom logger and initializing AWS service clients.

        :param solution_args: A dictionary containing required parameters for the solution, such as:
            - 'SOLUTION_ID': The unique identifier for the solution.
            - 'SOLUTION_VERSION': The current version of the solution.
            - 'RESOURCE_PREFIX': The prefix used to name and identify resources.
            - 'METRICS_NAMESPACE': The CloudWatch namespace used for logging custom metrics.
        """
        self.solution_id = solution_args['SOLUTION_ID']
        self.solution_version = solution_args['SOLUTION_VERSION']
        self.resource_prefix = solution_args['RESOURCE_PREFIX']
        self.metrics_namespace = solution_args['METRICS_NAMESPACE']
        self.logger = self.create_logger()
        self.s3_client = self.get_service_client("s3")
        self.cloudwatch_client = self.get_service_client('cloudwatch')
        
    def create_logger(self) -> logging.Logger:
        """
        Creates and configures a custom logger for the Glue job. The logger includes a custom format that displays
        the file path and line number for each log message.

        :return: A logger instance configured for Glue jobs.
        """
        formatter = logging.Formatter(
            "{%(pathname)s:%(lineno)d} %(levelname)s - %(message)s"
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        # Remove the default logger in order to avoid duplicate log messages
        # after we attach our custom logging handler.
        logging.getLogger().handlers.clear()
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        
        return logger
        
    def get_service_client(self, service_name: str) -> boto3.client:
        """
        Returns a boto3 client for the specified AWS service, configured with custom user-agent data.

        :param service_name: The name of the AWS service for which to create a client.
        :return: A boto3 client instance configured with the solution's user-agent.
        """
        botocore_config_defaults = {"user_agent_extra": f"AwsSolution/{self.solution_id}/{self.solution_version}"}
        amci_boto3_config = Config(**botocore_config_defaults)
        return boto3.client(service_name, config=amci_boto3_config)

    def get_service_resource(self, service_name: str) -> boto3.resource:
        """
        Returns a boto3 resource for the specified AWS service, configured with custom user-agent data.

        :param service_name: The name of the AWS service for which to create a resource.
        :return: A boto3 resource instance configured with the solution's user-agent.
        """
        botocore_config_defaults = {"user_agent_extra": f"AwsSolution/{self.solution_id}/{self.solution_version}"}
        amci_boto3_config = Config(**botocore_config_defaults)
        return boto3.resource(service_name, config=amci_boto3_config)
        
    def put_metrics_count_value_custom(self, metric_name: str, metric_value: int) -> None:
        """
        Record a custom metric with a specific value in CloudWatch.

        :param metric_name: The name of the metric to record.
        :param metric_value: The custom value to record for the metric.
        """
        try:
            self.logger.info(
                f"Recording metric {metric_name} and value {metric_value} in CloudWatch namespace {self.metrics_namespace}"
            )

            self.cloudwatch_client.put_metric_data(
                Namespace=self.metrics_namespace,
                MetricData=[
                    {
                        'MetricName': metric_name,
                        'Dimensions': [{'Name': 'stack-name', 'Value': self.resource_prefix}],
                        'Value': metric_value,
                        'Unit': 'Count'
                    }
                ]
            )
        except Exception as e:
            # Log error but do not raise so that execution is not interrupted
            self.logger.error(f"Error recording custom value {metric_value} to metric {metric_name}: {e}")
        
    def record_glue_metrics(self, 
                            source_bucket: str, 
                            destination_bucket: str, 
                            source_keys: list=[], 
                            destination_paths: list=[],
    ) -> None:
        """
        Records metrics for bytes read from the source S3 object and bytes written to the destination S3 object 
        during a Glue job transformation. Errors are logged but not raised to avoid interrupting the Glue job.

        :param source_bucket: The name of the S3 bucket containing the source objects.
        :param destination_bucket: The name of the S3 bucket containing the destination objects.
        :param source_keys (optional): A list of s3 keys for source objects.
        :param destination_paths (optional): A list of s3 key paths for destination objects that excludes the file name.
        """
        if not source_keys:
            self.logger.warning("No source keys provided for Glue job, skipping bytes_read metric")
            
        total_bytes_read = 0
        for key in source_keys:
            try:
                response = self.s3_client.head_object(Bucket=source_bucket, Key=key)
                bytes_read = response["ContentLength"]
                total_bytes_read += bytes_read
            except Exception as e:
                self.logger.error(f"Error retrieving bytes_read Glue metric for source_key {key}: {e}")
        if total_bytes_read > 0:
            self.put_metrics_count_value_custom("SdlfHeavyTransformJob-bytes_read", total_bytes_read)
        
        if not destination_paths:
            self.logger.warning("No destination paths provided for Glue job, skipping bytes_written metric")
        
        # Spark automatically names the files when writing them out. We create a custom prefix that includes
        # the source filename so that we can find all the files created from a specific run, but we need to iterate
        # over all the files in that prefix and sum them together (in most cases we expect just 1 file output
        # depending on the size of the source file).
        # <bucket-name>/<team>/<dataset>/<table_name>/<source_file_name>/output.parquet
        total_bytes_written = 0
        for path in destination_paths:
            try:
                response = self.s3_client.list_objects_v2(Bucket=destination_bucket, Prefix=path)
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        head_response = self.s3_client.head_object(Bucket=destination_bucket, Key=key)
                        bytes_written = head_response["ContentLength"]
                        total_bytes_written += bytes_written
            except Exception as e:
                self.logger.error(f"Error retrieving bytes_written Glue metric for destination_key {key}: {e}")
        if total_bytes_written > 0:
            self.put_metrics_count_value_custom("SdlfHeavyTransformJob-bytes_written", total_bytes_written)
    
    
    def get_s3_object_metadata(self, bucket_name: str, s3_key: str) -> dict:
        """
        Retrieves metadata from an S3 object.
        
        :param bucket_name: The name of the S3 bucket containing the object.
        :param s3_key: The key (path) of the S3 object within the bucket.

        :return: A dictionary containing the S3 object's metadata.
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            metadata = response.get('Metadata', {})
            return metadata
        
        except Exception as e:
            self.logger.error(f"Error retrieving metadata for s3://{bucket_name}/{s3_key}: {e}")
            raise e
    
    def return_timestamp(self, bucket_name: str, s3_key: str) -> str:
        """
        Retrieves the 'timestamp' metadata value from an S3 object, or generates a new
        timestamp if retrieval fails.

        :param bucket_name: The name of the S3 bucket containing the object.
        :param s3_key: The key (path) of the S3 object within the bucket.

        :return: The timestamp as a string in ISO 8601 format (e.g., '2024-08-27T12:34:56Z').
        """
        try:
            object_metadata = self.get_s3_object_metadata(bucket_name, s3_key)
            timestamp = object_metadata['timestamp']
            
            return timestamp
        
        except Exception:
            self.logger.error("Error retrieving timestamp. Generating one to use.")
            
            now_utc = dt.datetime.now(dt.timezone.utc)
            timestamp = now_utc.isoformat()
            
            return timestamp
    
    @staticmethod
    def map_fixed_value_column(record, col_name, col_val):
        """
        PySpark map function for adding a fixed value column to every record of a Dynamic Frame.
        
        Example of how to use: 
            df_dynamic.map(f = lambda record: map_fixed_value_column(record, col_name="timestamp", col_val=datetime))
        
        Documenation: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html#aws-glue-api-crawler-pyspark-extensions-dynamic-frame-map

        :param record: A Spark DynamicFrame record.
        :param col_name: The name of the column to add.
        :param col_val: The value of the column to add.

        :return: A Spark DynamicFrame record with an added fixed val column.
        """
        record[col_name] = col_val
        return record
    