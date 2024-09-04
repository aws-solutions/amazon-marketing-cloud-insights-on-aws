# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_solutions.core.helpers import get_service_resource
import logging


class Utils:
    def __init__(
            self,
            logger: logging.Logger = logging.Logger('common'),
    ):
        """
        Creates a new instance of the interface object based upon the configuration

        Parameters
        ----------
        logger:
            logger object for the class to use log info and error events
        """
        self.logger = logger

    def dynamodb_put_item(self, table_name: str, item: dict):
        self.logger.info(f'Creating item: {item} in table: {table_name}')
        dynamodb = get_service_resource('dynamodb')
        table = dynamodb.Table(table_name)

        try:
            response = table.put_item(Item=item)
            self.logger.info(f'Response: {response["ResponseMetadata"]["HTTPStatusCode"]}')
        except Exception as e:
            self.logger.error(f"Failed to write record {item} to Dynamodb table {table_name}")
            self.logger.error(e)

