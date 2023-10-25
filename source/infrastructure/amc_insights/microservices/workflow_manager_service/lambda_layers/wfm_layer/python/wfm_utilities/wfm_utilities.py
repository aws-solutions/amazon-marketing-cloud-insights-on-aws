# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_solutions.core.helpers import get_service_client, get_service_resource
import json
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from decimal import Decimal
import logging
import datetime as dt
import calendar
from dateutil.relativedelta import relativedelta
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
        logger
            logger object for the class to use log info and error events
        """
        self.logger = logger

    def deserialize_dynamodb_item(self, item: dict) -> dict:
        return {k: TypeDeserializer().deserialize(value=v) for k, v in item.items()}

    # This function will retrieve customer config records, you can specify the customer ID if you want a particular customer record
    def dynamodb_get_customer_config_records(self, dynamodb_table_name: str, customer_id: str = None) -> dict:
        customer_configs = {}
        # Set up a DynamoDB Connection
        dynamodb = get_service_client('dynamodb')

        if customer_id is None:
            # We want to get the whole table for all configs so we paginate in case there is large number of items
            paginator = dynamodb.get_paginator('scan')
            response_iterator = paginator.paginate(
                TableName=dynamodb_table_name, PaginationConfig={'PageSize': 100}
            )
        else:
            # paginate in case there is large number of items
            paginator = dynamodb.get_paginator('query')
            response_iterator = paginator.paginate(
                TableName=dynamodb_table_name,
                Select='ALL_ATTRIBUTES',
                Limit=1,
                ConsistentRead=False,
                ReturnConsumedCapacity='TOTAL',
                KeyConditions={
                    'customerId': {
                        'AttributeValueList': [
                            {
                                'S': customer_id
                            },
                        ],
                        'ComparisonOperator': 'EQ'
                    }
                }, PaginationConfig={'PageSize': 100})

        # Iterate over each page from the iterator
        for page in response_iterator:
            # deserialize each "item" (or record) into a client config dictionary
            if 'Items' in page:
                for item in page['Items']:
                    customer_config_item = self.deserialize_dynamodb_item(item)
                    # add the client config dictionary to our array
                    customer_configs[customer_config_item['customerId']] = customer_config_item.copy()
        return customer_configs

    def get_current_date_with_offset(
            self,
            offset_in_days: int,
            date_format: str = '%Y-%m-%dT00:00:00'  # NOSONAR
    ) -> str:
        """
        Returns the current date Plus or Minus the number of days specified as `offset_in_days`

        Parameters
        ----------
        offset_in_days:
            Positive or negative number of days to add to the current date
        date_format :
            Date format string to use for the return string value
        Returns
        -------
        str:
            Formatted date string

        """
        return ((dt.datetime.today() + dt.timedelta(days=offset_in_days)).strftime(date_format))


    def get_current_date_with_month_offset(
            self,
            offset_in_months: int
    ) -> dt.datetime:
        """
        Returns the current date Plus or Minus the number of months specified as `offset_in_months`

        Parameters
        ----------
        offset_in_months:
            Positive or negative number of days to add to the current date

        Returns
        -------
        datetime:
            datetime of the current date with the offset applied

        """
        return dt.datetime.today() + relativedelta(months=offset_in_months)

    def get_last_day_of_month(
            self,
            date: dt.datetime
    ) -> int:
        """
        finds the last day of the month for the specified `date`

        Parameters
        ----------
        date
            Date to find the last day of the month for

        Returns
        -------
        int:
            last day of the month for the month of the `date` specified


        """
        return calendar.monthrange(date.year, date.month)[1]

    def process_parameter_functions(
            self,
            parameter_value
    ) -> str:
        """
        Replaces values that have function names such as NOW() TODAY() LASTDAYOFOFFSETMONTH() FIRSTDAYOFOFFSETMONTH() FIFTEENTHDAYOFOFFSETMONTH()

        Parameters
        ----------
        parameter_value:
            AMC Parameter value to check for a function name and to process

        Returns
        -------
        str:
            returns the parameter value that was passed with the function evaluated. If no function was found the
            parameter value will be returned unchanged

        """
        if isinstance(parameter_value, str):
            if parameter_value.upper() == 'NOW()':
                return dt.datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

            if "TODAY(" in parameter_value.upper():
                if parameter_value.upper() == "TODAY()":
                    return self.get_current_date_with_offset(0)
                else:
                    return self.get_current_date_with_offset(self.get_offset_value(parameter_value))

            if "LASTDAYOFOFFSETMONTH(" in parameter_value.upper():
                date_with_month_offset = self.get_current_date_with_month_offset(
                    self.get_offset_value(parameter_value))
                last_day_of_previous_month = self.get_last_day_of_month(
                    date_with_month_offset)
                return_value = dt.datetime(date_with_month_offset.year, date_with_month_offset.month,
                                        last_day_of_previous_month,
                                        date_with_month_offset.hour, date_with_month_offset.minute).strftime(
                    '%Y-%m-%dT00:00:00')
                return return_value

            if "FIRSTDAYOFOFFSETMONTH(" in parameter_value.upper():
                date_with_month_offset = self.get_current_date_with_month_offset(
                    self.get_offset_value(parameter_value))
                return_value = dt.datetime(date_with_month_offset.year, date_with_month_offset.month, 1,
                                        date_with_month_offset.hour,
                                        date_with_month_offset.minute).strftime('%Y-%m-%dT00:00:00')
                return return_value

            if "FIFTEENTHDAYOFOFFSETMONTH(" in parameter_value.upper():
                date_with_month_offset = self.get_current_date_with_month_offset(
                    self.get_offset_value(parameter_value))
                return_value = dt.datetime(date_with_month_offset.year, date_with_month_offset.month, 15,
                                        date_with_month_offset.hour,
                                        date_with_month_offset.minute).strftime('%Y-%m-%dT00:00:00')
                return return_value
        # if no conditions are met, return the parameter unchanged
        return parameter_value


    def dynamodb_put_item(self, table_name: str, item: dict) -> bool:
        logger = self.logger
        logger.info(f'Creating item: {item} in table: {table_name}')
        dynamodb = get_service_resource('dynamodb')
        table = dynamodb.Table(table_name)

        response = ''
        try:
            response = table.put_item(Item=item)
            self.logger.info(f'Response: {response["ResponseMetadata"]["HTTPStatusCode"]}')
            return True

        except Exception as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                logger.error(e.response['Error']['Message'])
            else:
                self.logger.error("Failed to write record")
                self.logger.error(e.response['Error']['Code'])
                self.logger.error(e.response['Error']['Message'])
                return False

    # This function will decode anything that is not a string to a string, this is helpful when returning a json object
    def json_encoder_default(self, obj):

        if isinstance(obj, Decimal):
            return str(obj)

        if isinstance(obj, (dt.date, dt.datetime)):
            return obj.isoformat()

        if not isinstance(obj, str):
            return str(obj)

    def get_offset_value(self, offset_value: str) -> int:
        """
        Gets the value between parentheses as an integer

        Parameters
        ----------
        offset_value:
            The offset value to extract the integer from e.g. (1)

        Returns
        -------
        int:
            Integer value of the string that was between the parentheses

        """
        return int(offset_value.split('(')[1].split(')')[0])

    def is_json(self, text: str) -> bool:
        try:
            json.loads(text)
        except ValueError as e:
            self.logger.error(e)
            return False
        return True
