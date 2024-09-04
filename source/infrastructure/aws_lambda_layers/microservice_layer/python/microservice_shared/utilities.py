# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from collections.abc import Mapping
import logging
from decimal import Decimal
import datetime as dt

class LoggerUtil:
    """
    Utility class for creating and configuring a logger with a specific format.
    """
    def __init__(self):
        """
        Initializes the LoggerUtil instance and sets up the logger.
        """
        self.logger = LoggerUtil.create_logger()
    
    @staticmethod
    def create_logger():
        """
        Creates and configures a logger with a specific format.

        Returns
        -------
        logging.Logger
            Configured logger instance.
        """
        # format log messages like this:
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

class JsonUtil:
    """
    Utility class for handling JSON encoding and decoding with additional logging.
    """
    def __init__(self):
        """
        Initializes the JsonUtil instance and sets up the logger.
        """
        self.logger = LoggerUtil.create_logger()
    
    @staticmethod
    def json_encoder_default(obj):
        """
        Default JSON encoder for non-standard data types.

        Parameters
        ----------
        obj : Any
            Object to encode.

        Returns
        -------
        str
            JSON serializable representation of the object.
        """
        if isinstance(obj, Decimal):
            return str(obj)

        if isinstance(obj, (dt.date, dt.datetime)):
            return obj.isoformat()

        if not isinstance(obj, str):
            return str(obj)
    
    @staticmethod
    def safe_json_loads(obj):
        """
        Safely loads a JSON object from a string.

        Parameters
        ----------
        obj : str
            JSON string to decode.

        Returns
        -------
        Any
            Decoded JSON object, or the original string if decoding fails.
        """
        try:
            return json.loads(obj)
        except json.decoder.JSONDecodeError:
            return obj
        
    def is_json(self, text: str) -> bool:
        """
        Checks if a string is a valid JSON.

        Parameters
        ----------
        text : str
            String to check.

        Returns
        -------
        bool
            True if the string is a valid JSON, False otherwise.
        """
        try:
            json.loads(text)
        except ValueError as e:
            self.logger.error(e)
            return False
        return True

class DateUtil():
    """
    Utility class for handling datetime operations.
    """
    def __init__(self):
        """
        Initializes the DateUtil instance and sets up the logger.
        """
        self.logger = LoggerUtil.create_logger()

    @staticmethod
    def get_current_utc_iso_timestamp():
        """
        Creates a timestamp in ISO 8601 UTC format.
        """
        now_utc = dt.datetime.now(dt.timezone.utc)
        timestamp = now_utc.isoformat()
        
        return timestamp
        
class MapUtil():
    """
    Utility class for handling custom mapping functions.
    """
    def __init__(self):
        """
        Initializes the MapUtil instance and sets up the logger.
        """
        self.logger = LoggerUtil.create_logger()

    def map_nested_dicts_modify(
            self,
            dict_to_process,
            function_to_apply,
            **kwargs
    ) -> None:
        """
        Recursively applies a function to all values in a nested dictionary

        Parameters
        ----------
        dict_to_process:
            Dictionary to process
        function_to_apply:
            Function to apply to each value in the dictionary

        Returns
        -------
        None
        """
        for key, value in dict_to_process.items():
            if isinstance(value, Mapping):
                self.map_nested_dicts_modify(value, function_to_apply, **kwargs)
            else:
                dict_to_process[key] = function_to_apply(value, **kwargs)
