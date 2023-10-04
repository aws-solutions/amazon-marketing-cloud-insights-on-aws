# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from aws_solutions.core.helpers import get_service_client
from .base_config import BaseConfig
from ..commons import init_logger


class S3Configuration(BaseConfig):
    def __init__(self, resource_prefix, log_level=None, ssm_interface=None):
        """
        Complementary S3 config stores the S3 specific parameters
        :param log_level: level the class logger should log at
        :param ssm_interface: ssm interface, normally boto, to read parameters from parameter store
        """
        self.log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
        self._logger = init_logger(self.log_level)
        self._ssm = ssm_interface or get_service_client('ssm')
        self._resource_prefix = resource_prefix
        super().__init__(self.log_level, self._ssm)

        self._fetch_from_environment()
        self._fetch_from_ssm()

    def _fetch_from_environment(self):
        self._destination_bucket = os.getenv('BUCKET_TARGET', None)
        self._destination_encryption_key = os.getenv(
            'TARGET_ENCRYPTION_KEY', None)

    def _fetch_from_ssm(self):
        self._artifacts_bucket = None
        self._raw_bucket = None
        self._raw_bucket_kms_key = None
        self._stage_bucket = None
        self._stage_bucket_kms_key = None
        self._analytics_bucket = None
        self._analytics_bucket_kms_key = None

    @property
    def destination_bucket(self):
        return self._destination_bucket

    @property
    def destination_encryption_key(self):
        return self._destination_encryption_key

    @property
    def artifacts_bucket(self):
        if not self._artifacts_bucket:
            self._artifacts_bucket = self._get_ssm_param(
                '/{}/S3/ArtifactsBucket'.format(self._resource_prefix))
        return self._artifacts_bucket

    @property
    def raw_bucket(self):
        if not self._raw_bucket:
            self._raw_bucket = self._get_ssm_param(
                '/{}/S3/RawBucket'.format(self._resource_prefix))
        return self._raw_bucket

    @property
    def raw_bucket_kms_key(self):
        if not self._raw_bucket_kms_key:
            self._raw_bucket_kms_key = self._get_ssm_param(
                '/{}/KMS/RawBucket'.format(self._resource_prefix))
        return self._raw_bucket_kms_key

    @property
    def stage_bucket(self):
        if not self._stage_bucket:
            self._stage_bucket = self._get_ssm_param(
                '/{}/S3/StageBucket'.format(self._resource_prefix))
        return self._stage_bucket

    @property
    def stage_bucket_kms_key(self):
        if not self._stage_bucket_kms_key:
            self._stage_bucket_kms_key = self._get_ssm_param(
                '/{}/KMS/StageBucket'.format(self._resource_prefix))
        return self._stage_bucket_kms_key

    @property
    def analytics_bucket(self):
        if not self._analytics_bucket:
            self._analytics_bucket = self._get_ssm_param(
                '/{}/S3/AnalyticsBucket'.format(self._resource_prefix)
            ).split(':')[-1]
        return self._analytics_bucket

    @property
    def analytics_bucket_kms_key(self):
        if not self._analytics_bucket_kms_key:
            self._analytics_bucket_kms_key = self._get_ssm_param(
                '/{}/KMS/AnalyticsBucket'.format(self._resource_prefix))
        return self._analytics_bucket_kms_key


class DynamoConfiguration(BaseConfig):
    def __init__(self, resource_prefix, log_level=None, ssm_interface=None):
        """
        Complementary Dynamo config stores the parameters required to access dynamo tables
        :param log_level: level the class logger should log at
        :param ssm_interface: ssm interface, normally boto, to read parameters from parameter store
        """
        self.log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
        self._logger = init_logger(self.log_level)
        self._ssm = ssm_interface or get_service_client('ssm')
        self._resource_prefix = resource_prefix
        super().__init__(self.log_level, self._ssm)

        self._fetch_from_ssm()

    def _fetch_from_ssm(self):
        self._object_metadata_table = None
        self._transform_mapping_table = None

    @property
    def object_metadata_table(self):
        if not self._object_metadata_table:
            self._object_metadata_table = self._get_ssm_param(
                '/{}/DynamoDB/ObjectMetadata'.format(self._resource_prefix))
        return self._object_metadata_table

    @property
    def transform_mapping_table(self):
        if not self._transform_mapping_table:
            self._transform_mapping_table = self._get_ssm_param(
                '/{}/DynamoDB/Datasets'.format(self._resource_prefix))
        return self._transform_mapping_table


class SQSConfiguration(BaseConfig):
    def __init__(self, resource_prefix, team, dataset, stage, log_level=None, ssm_interface=None):
        """
        Complementary SQS config stores the parameters required to access SQS
        :param log_level: level the class logger should log at
        :param ssm_interface: ssm interface, normally boto, to read parameters from parameter store
        """
        self.log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
        self._logger = init_logger(self.log_level)
        self._ssm = ssm_interface or get_service_client('ssm')
        self._team = team
        self._dataset = dataset
        self._stage = stage
        self._resource_prefix = resource_prefix
        super().__init__(self.log_level, self._ssm)

        self._fetch_from_ssm()

    def _fetch_from_ssm(self):
        self._stage_queue_name = None
        self._stage_dlq_name = None

    @property
    def get_stage_queue_name(self):
        if not self._stage_queue_name:
            self._stage_queue_name = self._get_ssm_param(
                '/{}/SQS/{}/{}{}Queue'.format(self._resource_prefix, self._team, self._dataset, self._stage))
        return self._stage_queue_name

    @property
    def get_stage_dlq_name(self):
        if not self._stage_dlq_name:
            self._stage_dlq_name = self._get_ssm_param(
                '/{}/SQS/{}/{}{}DLQ'.format(self._resource_prefix, self._team, self._dataset, self._stage))
        return self._stage_dlq_name


class StateMachineConfiguration(BaseConfig):
    def __init__(self, resource_prefix, team, pipeline, stage, log_level=None, ssm_interface=None):
        """
        Complementary State Machine config stores the parameters required to access State Machines
        :param log_level: level the class logger should log at
        :param ssm_interface: ssm interface, normally boto, to read parameters from parameter store
        """
        self.log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
        self._logger = init_logger(self.log_level)
        self._ssm = ssm_interface or get_service_client('ssm')
        self._team = team
        self._pipeline = pipeline
        self._stage = stage
        self._resource_prefix = resource_prefix
        super().__init__(self.log_level, self._ssm)

        self._fetch_from_ssm()

    def _fetch_from_ssm(self):
        self._stage_state_machine_arn = None

    @property
    def get_stage_state_machine_arn(self):
        if not self._stage_state_machine_arn:
            self._stage_state_machine_arn = self._get_ssm_param(
                '/{}/SM/{}/{}{}SM'.format(self._resource_prefix, self._team, self._pipeline, self._stage))
        return self._stage_state_machine_arn


class KMSConfiguration(BaseConfig):
    def __init__(self, resource_prefix, name, log_level=None, ssm_interface=None):
        """
        Complementary KMS config stores the parameters required to access CMKs
        :param log_level: level the class logger should log at
        :param ssm_interface: ssm interface, normally boto, to read parameters from parameter store
        """
        self.log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
        self._logger = init_logger(self.log_level)
        self._ssm = ssm_interface or get_service_client('ssm')
        self._name = name
        self._resource_prefix = resource_prefix
        super().__init__(self.log_level, self._ssm)

        self._fetch_from_ssm()

    def _fetch_from_ssm(self):
        self._kms_arn = None

    @property
    def get_kms_arn(self):
        if not self._kms_arn:
            self._kms_arn = self._get_ssm_param(
                '/{}/KMS/{}BucketKeyArn'.format(self._resource_prefix, self._name))
        return self._kms_arn
