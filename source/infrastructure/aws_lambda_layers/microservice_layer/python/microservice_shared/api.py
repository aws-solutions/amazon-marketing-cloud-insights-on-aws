# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import urlencode
import urllib3
from urllib3.util import Retry
from dataclasses import dataclass

from microservice_shared.utilities import LoggerUtil


@dataclass
class RequestParams:
    """
    Class for keeping track of parameters needed to make an API Request
    
    Parameters
    ----------
    http_method : str
        The HTTP method for the request (GET, POST, etc.).
    request_path : str, optional
        The path for the request, by default None. If not provided, request_url must be provided.
    request_url : str, optional
        The URL for the request, by default None. If not provided, request_path must be provided.
    payload : str, optional
        The payload for the request, by default None.
    request_parameters : dict, optional
        The request parameters, by default None.
    """
    def __init__(
            self,
            http_method,
            request_path=None,
            request_url=None,
            payload=None,
            request_parameters=None,
    ) -> None:
        self.http_method = http_method.upper()
        self.request_path = f"/{request_path}".replace("//", "/")
        self.request_url = request_url or ""
        self.payload = payload or ""
        self.request_parameters = request_parameters or {}
        
        if not request_url and not request_path:
            raise ValueError("Either request_url or request_path must be provided.")

class ApiHelper:
    """
    Helper class for making HTTP requests.
    """
    def __init__(self):
        """
        Initializes the ApiHelper instance.
        """
        self.logger = LoggerUtil.create_logger()
    
    def encode_query_parameters_to_url(self, url, query_parameters):
        """
        Encode query parameters into a URL.

        Parameters
        ----------
        url : str
            The base URL.
        query_parameters : dict
            The query parameters to encode.

        Returns
        -------
        str
            The encoded URL with query parameters.
        """
        if query_parameters:
            encoded_url = url + "?" + urlencode(query_parameters)
            self.logger.info(f"Request URL with encoded query parameters= {encoded_url}")
            return encoded_url
        return url
    
    def send_request(self, request_url, headers, http_method, data, query_params, log_request_data=True) -> urllib3.HTTPResponse:
        """
        Sends an HTTP request to the target API.

        Parameters
        ----------
        http_method : str
            The HTTP method to use (GET, PUT, POST, DELETE).
        request_url : str
            The URL of the endpoint to send the HTTP request to.
        data : str
            The body to include in the HTTP request.
        headers : None or dict
            The request headers.
        query_params: dict
            The query parameters.
        log_request_data: bool
            Whether or not to log request data.

        Returns
        -------
        urllib3.HTTPResponse
            A response from the HTTP call.
        """

        self.logger.info("\nBEGIN REQUEST+++++++++++++++++++++++++++++++++++")
        self.logger.info(f"Request URL = {request_url}")
        self.logger.info(f"HTTP_METHOD: {http_method}")
        if log_request_data:
            self.logger.info(f"Data: {data}")
            self.logger.info(f"Query Parameters: {query_params}")

        # Retry requests that receive server error (5xx) or throttling errors 429.
        max_retry = 10
        retries = Retry(
            total=max_retry,
            backoff_factor=0.5,
            status_forcelist=[504, 500, 429],
            allowed_methods=frozenset(["GET", "DELETE", "POST", "PUT"]),
        )

        http = urllib3.PoolManager(retries=retries)

        if http_method in ["GET", "HEAD", "DELETE"]:
            response = http.request(
                method=http_method,
                url=request_url,
                headers=headers,
                body=data,
                fields=query_params,
            )
        else:
            # For POST and PUT requests, urllib3 requires to manually encode query parameters in the URL
            request_url = self.encode_query_parameters_to_url(request_url, query_params)

            response = http.request(
                method=http_method,
                url=request_url,
                headers=headers,
                body=data,
            )

        self.logger.info("\nRESPONSE+++++++++++++++++++++++++++++++++++")
        self.logger.info(f"Response status: {response.status}\n")
        if log_request_data:
            self.logger.info(f"Response data: {response.data}\n")

        return response
