# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from typing import Union
from datetime import datetime
import urllib3
from botocore import config
import urllib
from microservice_shared.secrets import SecretsHelper
from microservice_shared.utilities import JsonUtil, LoggerUtil
from microservice_shared.api import ApiHelper, RequestParams

SOLUTION_VERSION = os.environ.get(
    "VERSION", os.environ.get("SOLUTION_VERSION")
)
REGION = os.environ["REGION"]
SOLUTION_ID = os.environ["SOLUTION_ID"]
solution_config = {"region_name": REGION, "user_agent_extra": f"AwsSolution/{SOLUTION_ID}/{SOLUTION_VERSION}"}
config = config.Config(**solution_config)

logger = LoggerUtil.create_logger()
json_helper = JsonUtil()
api_helper = ApiHelper()


class ExceptionResponse:
    """Used to return a standard structured response if exception captured before making http requests to Amazon Ads"""

    def __init__(self, is_success, response_status, response_message):
        self.success = is_success or False
        self.response = {
            "responseStatus": response_status or "FAILED",
            'responseMessage': response_message or "Check state machine logs for more detail"
        }


class AmazonAdsReportingAPIResponse:
    """Used to return a standard structured response by processing the response from the Amazon Ads Reporting API"""

    def __init__(self, response: urllib3.HTTPResponse):
        """
        Initialize the AmazonAdsReportingAPIResponse.

        Parameters
        ----------
        response : urllib3.HTTPResponse
            The HTTP response from the Amazon Ads Reporting API.
        """
        self.http_response = response
        self.response_data = json.loads(response.data.decode("utf-8"))
        self.status_code = response.status
        self.response_status = self.response_data.get('status') or str(self.status_code)
        self.response_received_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.response_text = response.data.decode("utf-8")

        logger.info("\nBEGIN STRUCTURED HTTP RESPONSE+++++++++++++++++++++++++++++++++++")

        self.response = {"responseReceivedTime": self.response_received_time,
                         "responseStatus": self.response_status, "statusCode": self.status_code,
                         "requestURL": response.geturl()}

        if json_helper.is_json(self.response_text):
            self.response.update(json.loads(self.response_text))

        if self.status_code in range(200, 204):
            self.success = True
            # Remove status from response and rename to responseStatus because status is a reserved word in DynamoDB
            if 'status' in self.response:
                del self.response['status']
        else:
            self.success = False
            self.response['responseStatus'] = 'FAILED'

        logger.info(f"Structured response: {self.response}")
        logger.info("\nEND STRUCTURED HTTP RESPONSE+++++++++++++++++++++++++++++++++++")

    def log_response(self):
        if self.success:
            logger.info(self.response)
        else:
            logger.error(self.response)


class AmazonAdsAPIs:
    """
    Used to interact with Amazon Ads reporting API Endpoint by making HTTP Requests for
    create report and check report status requests
    """

    def __init__(self, region, auth_id=None):
        """
        Creates a new instance of the interface object based upon Amazon Ads reporting feature
        
        Parameters
        ----------
        region : str
            The region for the request.
        auth_id : str, optional
            The authorization Id key for the token credentials.
        """
        self.region = region
        self.auth_id = auth_id
        
        self.secrets_helper = SecretsHelper(
            secret_key=os.environ["AMAZON_ADS_SECRETS_MANAGER"],
            auth_id=self.auth_id
        )
        
    def verify_amazon_ads_request(self) -> dict:
        clients_and_tokens = self.secrets_helper.get_access_token()
        if clients_and_tokens.get("authorize_url"):
            raise RuntimeError("Unauthorized AMC request.")
        return clients_and_tokens

    def process_request(self, ads_request, kwargs, return_raw=False) -> Union[AmazonAdsReportingAPIResponse, dict]:
        """
        Prepare HTTP headers then make HTTP requests.

        Parameters
        ----------
        ads_request : RequestParams
            RequestParams object containing the necessary parameters for the AMC API request.
        kwargs : dict
            Dictionary containing Client ID, Access Token, and Profile ID
        return_raw : bool, optional
            Flag to return raw response instead of structured response, by default False

        Returns
        -------
        Union[AmazonAdsReportingAPIResponse, dict]
            Structure Response from the Amazon Ads API or raw response if return_raw is True.
        """
        # we initialize default headers while checking for additional/custom values as well
        # api calls to the reporting endpoint require profile_id while the profile endpoint does not
        headers = {
            "Amazon-Advertising-API-ClientId": kwargs["client_id"],
            "Authorization": f'Bearer {kwargs["access_token"]}',
            "Content-Type": kwargs.get("Content-Type", "application/json"),
            "x-amzn-service-name": "amazon-marketing-cloud-insights-on-aws",
            "x-amzn-service-version": SOLUTION_VERSION
        }
        if kwargs.get("profile_id"):
            headers["Amazon-Advertising-API-Scope"] = kwargs["profile_id"]

        logger.debug(f"AMAZON_ADS_REPORT_REQUEST_URL: {ads_request.request_url}")
        logger.debug(f"AMAZON_ADS_REPORT_REQUEST_PAYLOAD: {ads_request.payload}")
        logger.debug(f"AMAZON_ADS_REPORT_HTTP_METHOD: {ads_request.http_method}")
        logger.debug(f"AMAZON_ADS_REPORT_REQUEST_PARAMETERS: {ads_request.request_parameters}")

        # Use client id and access token stored in Secrets Manager to authorize requests.
        # If requests are Unauthorized (status code 401), refresh the access token, then make request again using
        # the new access token.
        response = api_helper.send_request(
            request_url=ads_request.request_url,
            headers=headers,
            http_method=ads_request.http_method,
            data=ads_request.payload,
            query_params=ads_request.request_parameters,
        )

        if response.status == 401:
            logger.info(
                f"Request to {ads_request.request_url} is Unauthorized (status code 401), refresh access token, then try again")
            tokens = self.verify_amazon_ads_request()
            headers["Authorization"] = f'Bearer {tokens["access_token"]}'

            response = api_helper.send_request(
                request_url=ads_request.request_url,
                headers=headers,
                http_method=ads_request.http_method,
                data=ads_request.payload,
                query_params=ads_request.request_parameters,
            )

        if return_raw:
            return json.loads(response.data.decode("utf-8"))
        else:
            return AmazonAdsReportingAPIResponse(response)

    def get_ads_parameters(self) -> dict:
        """
        Retrieves values from secrets manager.

        Returns
        -------
            dict: Dictionary containing the necessary parameters for Amazon Ads reporting.
                Keys include 'client_id', and 'access_token'.
        """
        secrets = self.secrets_helper.get_secret()
        self.secrets_helper.validate_secrets(secrets)

        client_id = secrets.get("client_id")
        access_token = secrets.get("access_token", "")

        ads_parameters = {
            "client_id": client_id,
            "access_token": access_token,
        }

        return ads_parameters

    def request_sponsored_ads_v3_reporting(
            self,
            version_3_reporting_data,
            profile_id: str,
    ) -> Union[AmazonAdsReportingAPIResponse, ExceptionResponse]:
        """
        Make a request for sponsored ads V3 reporting.

        Parameters
        ----------
        version_3_reporting_data : dict
            The reporting data for version 3.
        profile_id : str
            The profile ID for the request.

        Returns
        -------
        Union[AmazonAdsReportingAPIResponse, ExceptionResponse]
            The response from the Amazon Ads API or a ExceptionResponse in case of an error.
        """
        try:
            request_body = json.dumps(version_3_reporting_data, default=json_helper.json_encoder_default)

            ads_kwargs = self.get_ads_parameters()
            ads_kwargs.update({
                "profile_id": profile_id
            })
            ads_kwargs.update({
                "Content-Type": "application/vnd.createasyncreportrequest.v3+json"
            })

            request_url = AmazonAdsReportingUrlBuilder(self.region).get_sponsored_ads_v3_reporting_url()

            amazon_ads_request = RequestParams(
                request_url=request_url,
                http_method="POST",
                payload=request_body,
            )

            amazon_ads_response = self.process_request(amazon_ads_request, ads_kwargs)

            amazon_ads_response.log_response()
            return amazon_ads_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return ExceptionResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    def report_status(
        self, 
        report_id: str,
        profile_id: str,
    ) -> Union[AmazonAdsReportingAPIResponse, ExceptionResponse]:
        """
        Check the status of a report.

        Parameters
        ----------
        report_id : str
            The ID of the report.
        profile_id : str
            The profile ID for the request.

        Returns
        -------
        Union[AmazonAdsReportingAPIResponse, ExceptionResponse]
            The response from the Amazon Ads API or a ExceptionResponse in case of an error.
        """
        try:
            ads_kwargs = self.get_ads_parameters()
            ads_kwargs.update({
                "profile_id": profile_id
            })
            ads_kwargs.update({
                "Content-Type": "application/vnd.createasyncreportrequest.v3+json"
            })

            request_url = AmazonAdsReportingUrlBuilder(self.region).get_report_status_url(report_id)

            amazon_ads_request = RequestParams(
                request_url=request_url,
                http_method="GET",
            )

            amazon_ads_response = self.process_request(amazon_ads_request, ads_kwargs)

            amazon_ads_response.log_response()
            return amazon_ads_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return ExceptionResponse(is_success=False, response_status="FAILED", response_message=repr(ex))
        
    def get_profiles_by_region(
        self,
    ) -> Union[list, ExceptionResponse]:
        """
        Get the list of profiles for a particular region.

        Parameters
        ----------
        region : str
            The region for the request.

        Returns
        -------
        Union[list, ExceptionResponse]
            A list of profiles or a ExceptionResponse in case of an error.
        """
        try:
            ads_kwargs = self.get_ads_parameters()

            request_url = AmazonAdsReportingUrlBuilder(self.region).get_profiles_url()

            amazon_ads_request = RequestParams(
                request_url=request_url,
                http_method="GET",
            )

            amazon_ads_response = self.process_request(ads_request=amazon_ads_request,kwargs=ads_kwargs, return_raw=True)
            return amazon_ads_response
        except Exception as ex:
            logger.error(ex)
            return ExceptionResponse(is_success=False, response_status="FAILED", response_message=repr(ex))


class AmazonAdsReportingUrlBuilder:
    def __init__(self, region):
        self.base_url = self.get_base_url(region)
    
    @staticmethod
    def get_base_url(region):
        """Returns the base url for a given Amazon Ads region"""
        
        url_region_map = {
            "North America": "https://advertising-api.amazon.com",
            "Europe": "https://advertising-api-eu.amazon.com",
            "APAC": "https://advertising-api-fe.amazon.com"
        }
        try:
            return url_region_map[region]
        except KeyError:
            raise ValueError(f"Invalid region '{region}'. Supported regions are: {list(url_region_map.keys())}")

    def get_report_status_url(self, report_id: str) -> str:
        url_path = f"reporting/reports/{report_id}"
        return urllib.parse.urljoin(self.base_url, url_path)

    def get_sponsored_ads_v3_reporting_url(self) -> str:
        return urllib.parse.urljoin(self.base_url, "reporting/reports")

    def get_profiles_url(self) -> str:
        return urllib.parse.urljoin(self.base_url, "/v2/profiles")
