# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime as dt
import calendar
from dateutil.relativedelta import relativedelta

from microservice_shared.utilities import LoggerUtil

class DynamicDateEvaluator():
    """
    Class for handling dynamic date functions that can be included in request payloads 
    and are resolved at runtime.
    """
    def __init__(self):
        """
        Initializes the DynamicDateEvaluator instance and sets up the logger.
        """
        self.logger = LoggerUtil.create_logger()

    @staticmethod
    def get_current_date_with_offset(
            offset_in_days: int,
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
        return (dt.datetime.today() + dt.timedelta(days=offset_in_days))

    @staticmethod
    def get_current_date_with_month_offset(
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

    @staticmethod
    def get_last_day_of_month(
            date: dt.datetime
    ) -> int:
        """
        finds the last day of the month for the specified `date`

        Parameters
        ----------
        date:
            Date to find the last day of the month for

        Returns
        -------
        int:
            last day of the month for the month of the `date` specified


        """
        return calendar.monthrange(date.year, date.month)[1]
    
    @staticmethod
    def get_offset_value(offset_value: str) -> int:
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

    def process_parameter_functions(
            self,
            parameter_value,
            date_format: str = '%Y-%m-%dT00:00:00'  # NOSONAR
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
            parameter_value_in_uppercase = parameter_value.upper()

            if parameter_value_in_uppercase == 'NOW()':
                return dt.datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

            if "TODAY(" in parameter_value_in_uppercase:
                if parameter_value.upper() == "TODAY()":
                    offset_days = 0
                    return self.get_current_date_with_offset(offset_days).strftime(date_format)
                else:
                    offset_days = self.get_offset_value(parameter_value)
                    return self.get_current_date_with_offset(offset_days).strftime(date_format)

            if "LASTDAYOFOFFSETMONTH(" in parameter_value_in_uppercase:
                date_with_month_offset = self.get_current_date_with_month_offset(
                    self.get_offset_value(parameter_value))
                last_day_of_previous_month = self.get_last_day_of_month(
                    date_with_month_offset)
                return dt.datetime(date_with_month_offset.year, date_with_month_offset.month,
                                   last_day_of_previous_month,
                                   date_with_month_offset.hour, date_with_month_offset.minute).strftime(
                    date_format)

            if "FIRSTDAYOFOFFSETMONTH(" in parameter_value_in_uppercase:
                date_with_month_offset = self.get_current_date_with_month_offset(
                    self.get_offset_value(parameter_value))
                return dt.datetime(date_with_month_offset.year, date_with_month_offset.month, 1,
                                   date_with_month_offset.hour,
                                   date_with_month_offset.minute).strftime(date_format)

            if "FIFTEENTHDAYOFOFFSETMONTH(" in parameter_value_in_uppercase:
                date_with_month_offset = self.get_current_date_with_month_offset(
                    self.get_offset_value(parameter_value))
                return dt.datetime(date_with_month_offset.year, date_with_month_offset.month, 15,
                                   date_with_month_offset.hour,
                                   date_with_month_offset.minute).strftime(date_format)

        # if no conditions are met, return the parameter unchanged
        return parameter_value