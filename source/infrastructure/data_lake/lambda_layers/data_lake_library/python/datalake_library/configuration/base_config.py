# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from botocore.exceptions import ClientError

from ..commons import init_logger


class BaseConfig:
    def __init__(self, log_level, ssm_interface):
        self.log_level = log_level
        self._logger = init_logger(log_level)
        self._ssm = ssm_interface

    def _fetch_from_event(self):
        raise NotImplementedError()

    def _fetch_from_environment(self):
        raise NotImplementedError()

    def _fetch_from_ssm(self):
        raise NotImplementedError()

    def _fetch_from_dynamodb(self):
        raise NotImplementedError()

    def _get_ssm_param(self, key):
        try:
            self._logger.info('Obtaining SSM Parameter: {}'.format(key))
            ssm_parameter_value = self._ssm.get_parameter(Name=key)['Parameter']['Value']
            self._logger.info('Obtained SSM Parameter: Key: {} Value: {}'.format(key, ssm_parameter_value))
            return ssm_parameter_value
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                self._logger.error("SSM RATE LIMIT REACHED")
            else:
                self._logger.error("Unexpected error: %s" % e)
            raise
