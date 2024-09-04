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
    """Used to return a standard structured response if exception captured before making http requests to SP-API"""

    def __init__(self, is_success, response_status, response_message):
        self.success = is_success or False
        self.response = {
            "responseStatus": response_status or "FAILED",
            'responseMessage': response_message or "Check state machine logs for more detail"
        }


class SellingPartnerReportingAPIResponse:
    """Used to return a standard structured response by processing the response from the SP-API Reporting API"""

    def __init__(self, response: urllib3.HTTPResponse):
        """
        Initialize the SellingPartnerReportingAPIResponse.

        Parameters
        ----------
        response : urllib3.HTTPResponse
            The HTTP response from the SP-API Reporting API.
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


class SellingPartnerAPI:
    """
    Used to interact with SP-API reporting API Endpoint by making HTTP Requests for
    create report and check report status requests
    """

    def __init__(self, region, auth_id=None):
        """
        Creates a new instance of the interface object based upon SP-API reporting feature
        
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
            secret_key=os.environ["SELLING_PARTNER_SECRETS_MANAGER"],
            auth_id=self.auth_id
        )
        
    def verify_selling_partner_request(self) -> dict:
        clients_and_tokens = self.secrets_helper.get_access_token()
        if clients_and_tokens.get("authorize_url"):
            raise RuntimeError("Unauthorized AMC request.")
        return clients_and_tokens

    def process_request(self, request_params, kwargs, return_raw=False) -> Union[SellingPartnerReportingAPIResponse, dict]:
        """
        Prepare HTTP headers then make HTTP requests.

        Parameters
        ----------
        request_params : RequestParams
            RequestParams object containing the necessary parameters for the API request.
        kwargs : dict
            Dictionary containing Client ID, Access Token, and Profile ID
        return_raw : bool, optional
            Flag to return raw response instead of structured response, by default False

        Returns
        -------
        Union[SellingPartnerReportingAPIResponse, dict]
            Structure Response from the SP-API or raw response if return_raw is True.
        """
        # we initialize default headers while checking for additional/custom values as well
        # api calls to the reporting endpoint require profile_id while the profile endpoint does not
        headers = {
            "x-amz-access-token": kwargs["access_token"],
            "Content-Type": kwargs.get("Content-Type", "application/json"),
            "x-amzn-service-name": "amazon-marketing-cloud-insights-on-aws",
            "x-amzn-service-version": SOLUTION_VERSION
        }

        logger.debug(f"SELLING_PARTNER_REQUEST_URL: {request_params.request_url}")
        logger.debug(f"SELLING_PARTNER_HTTP_METHOD: {request_params.http_method}")
        logger.debug(f"SELLING_PARTNER_REQUEST_PAYLOAD: {request_params.payload}")
        logger.debug(f"SELLING_PARTNER_REQUEST_PARAMETERS: {request_params.request_parameters}")

        # Use client id and access token stored in Secrets Manager to authorize requests.
        # If requests are Unauthorized (status code 401), refresh the access token, then make request again using
        # the new access token.
        response = api_helper.send_request(
            request_url=request_params.request_url,
            headers=headers,
            http_method=request_params.http_method,
            data=request_params.payload,
            query_params=request_params.request_parameters,
        )

        # response codes: https://developer-docs.amazon.com/sp-api/docs/reports-api-v2021-06-30-reference
        if response.status in [401, 403]:
            logger.info(
                f"Request to {request_params.request_url} is Unauthorized (status code {response.status}), refresh access token, then try again")
            tokens = self.verify_selling_partner_request()
            headers["x-amz-access-token"] = tokens["access_token"]

            response = api_helper.send_request(
                request_url=request_params.request_url,
                headers=headers,
                http_method=request_params.http_method,
                data=request_params.payload,
                query_params=request_params.request_parameters,
            )

        if return_raw:
            return json.loads(response.data.decode("utf-8"))
        else:
            return SellingPartnerReportingAPIResponse(response)

    def get_auth_parameters(self) -> dict:
        """
        Retrieves values from secrets manager.

        Returns
        -------
            dict: Dictionary containing the necessary parameters for SP-API authorization.
                Keys include 'access_token'.
        """
        secrets = self.secrets_helper.get_secret()
        self.secrets_helper.validate_secrets(secrets)

        access_token = secrets.get("access_token", "")

        auth_parameters = {
            "access_token": access_token,
        }

        return auth_parameters

    def create_report(
            self,
            request_body,
    ) -> Union[SellingPartnerReportingAPIResponse, ExceptionResponse]:
        """
        Make a request for sponsored ads report.

        Parameters
        ----------
        report_data : dict
            The reporting data for version 3.

        Returns
        -------
        Union[SellingPartnerReportingAPIResponse, ExceptionResponse]
            The response from the SP-API or a ExceptionResponse in case of an error.
        """
        try:
            request_body = json.dumps(request_body, default=json_helper.json_encoder_default)

            ads_kwargs = self.get_auth_parameters()

            request_url = SellingPartnerReportingUrlBuilder(self.region).get_create_report_url()

            selling_partner_request = RequestParams(
                request_url=request_url,
                http_method="POST",
                payload=request_body,
            )

            selling_partner_response = self.process_request(selling_partner_request, ads_kwargs)

            selling_partner_response.log_response()
            return selling_partner_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return ExceptionResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    def get_report_status(
        self,
        report_id: str,
    ) -> Union[SellingPartnerReportingAPIResponse, ExceptionResponse]:
        """
        Get the status of a report.

        Parameters
        ----------
        report_id : str
            The ID of the report.

        Returns
        -------
        Union[SellingPartnerReportingAPIResponse, ExceptionResponse]
            The response from the SP-API or a ExceptionResponse in case of an error.
        """
        try:
            ads_kwargs = self.get_auth_parameters()

            request_url = SellingPartnerReportingUrlBuilder(self.region).get_report_status_url(report_id)

            selling_partner_request = RequestParams(
                request_url=request_url,
                http_method="GET",
            )

            selling_partner_response = self.process_request(selling_partner_request, ads_kwargs)

            selling_partner_response.log_response()
            return selling_partner_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return ExceptionResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    def get_report_document(
        self,
        report_document_id: str,
    ) -> Union[SellingPartnerReportingAPIResponse, ExceptionResponse]:
        """
        Get the URL to download a report document.

        Parameters
        ----------
        report_document_id : str
            The ID of the report document.

        Returns
        -------
        Union[SellingPartnerReportingAPIResponse, ExceptionResponse]
            The response from the SP-API or a ExceptionResponse in case of an error.
        """
        try:
            ads_kwargs = self.get_auth_parameters()

            request_url = SellingPartnerReportingUrlBuilder(self.region).get_report_document_url(report_document_id)

            selling_partner_request = RequestParams(
                request_url=request_url,
                http_method="GET",
            )

            selling_partner_response = self.process_request(selling_partner_request, ads_kwargs)

            selling_partner_response.log_response()
            return selling_partner_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return ExceptionResponse(is_success=False, response_status="FAILED", response_message=repr(ex))


class SellingPartnerReportingUrlBuilder:
    def __init__(self, region):
        self.base_url = self.get_base_url(region)
    
    @staticmethod
    def get_base_url(region):
        """Returns the base url for a given SP-API region"""
        # Reference: https://developer-docs.amazon.com/sp-api/docs/sp-api-endpoints
        url_region_map = {
            "North America": "https://sellingpartnerapi-na.amazon.com",
            "Europe": "https://sellingpartnerapi-eu.amazon.com",
            "Far East": "https://sellingpartnerapi-fe.amazon.com"
        }
        try:
            return urllib.parse.urljoin(url_region_map[region], "reports/2021-06-30/")
        except KeyError:
            raise ValueError(f"Invalid region '{region}'. Supported regions are: {list(url_region_map.keys())}")

    def get_create_report_url(self) -> str:
        url_path = f"reports/"
        return urllib.parse.urljoin(self.base_url, url_path)

    def get_report_status_url(self, report_id: str) -> str:
        url_path = f"reports/{report_id}"
        return urllib.parse.urljoin(self.base_url, url_path)

    def get_report_document_url(self, report_document_id: str) -> str:
        url_path = f"documents/{report_document_id}"
        return urllib.parse.urljoin(self.base_url, url_path)
