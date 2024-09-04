# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from boto3.dynamodb.types import TypeDeserializer

from aws_solutions.core.helpers import get_service_resource
from microservice_shared.utilities import LoggerUtil

class DynamodbHelper:
    """
    Helper class for interacting with AWS DynamoDB.
    """
    def __init__(self):
        """
        Initializes the DynamodbHelper instance.
        """
        self.logger = LoggerUtil.create_logger()
        
    @staticmethod
    def deserialize_dynamodb_item(item: dict) -> dict:
        """
        Deserialize a DynamoDB item into a standard Python dictionary.

        Parameters
        ----------
        item : dict
            A dictionary representing a DynamoDB item.

        Returns
        -------
        dict
            A dictionary with the deserialized DynamoDB item.
        """
        return {k: TypeDeserializer().deserialize(value=v) for k, v in item.items()}
    
    def dynamodb_put_item(self, table_name: str, item: dict):
        """
        Put an item into a DynamoDB table.

        Parameters
        ----------
        table_name : str
            The name of the DynamoDB table.
        item : dict
            The item to put into the table.

        Raises
        ------
        Exception
            If there is an error putting the item into the DynamoDB table.
        """
        self.logger.info(f'Creating item: {item} in table: {table_name}')
        dynamodb = get_service_resource('dynamodb')
        table = dynamodb.Table(table_name)

        try:
            response = table.put_item(Item=item)
            self.logger.info(f'Response: {response["ResponseMetadata"]["HTTPStatusCode"]}')
        except Exception as e:
            self.logger.error(f"Failed to write record {item} to DynamoDB table {table_name}")
            self.logger.error(e)
