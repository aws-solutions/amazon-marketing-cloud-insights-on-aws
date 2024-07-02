# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
from typing import Union
from datetime import datetime, timedelta
from urllib.parse import urlencode
import urllib3
from urllib3.util import Retry
import boto3
from botocore import config
import urllib
from wfm_utilities import wfm_utilities

SOLUTION_VERSION = os.environ.get(
    "VERSION", os.environ.get("SOLUTION_VERSION")
)
REGION = os.environ["REGION"]
SOLUTION_ID = os.environ["SOLUTION_ID"]
solution_config = {"region_name": REGION, "user_agent_extra": f"AwsSolution/{SOLUTION_ID}/{SOLUTION_VERSION}"}
config = config.Config(**solution_config)

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


def send_request(request_url, headers, http_method, data, query_params, log_request_data=True) -> urllib3.HTTPResponse:
    """
    Sends an HTTP request to the Amazon Ads API

    Parameters
    ----------
    http_method : str
        GET|PUT|POST|DELETE
    request_url : str
        The URL of the endpoint to send the http request to
    data : str
        Body to include in the HTTP request
    headers : None or dict
        Request headers
    query_params: dict
        Query parameters
    log_request_data: bool
        Whether or not to log request data

    Returns
    -------
        A response from the HTTP call
    """

    logger.info("\nBEGIN REQUEST+++++++++++++++++++++++++++++++++++")
    logger.info(f"Request URL = {request_url}")
    logger.info(f"HTTP_METHOD: {http_method}")
    if log_request_data:
        logger.info(f"Data: {data}")
        logger.info(f"Query Parameters: {query_params}")

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
        request_url = encode_query_parameters_to_url(request_url, query_params)

        response = http.request(
            method=http_method,
            url=request_url,
            headers=headers,
            body=data,
        )

    logger.info("\nRESPONSE+++++++++++++++++++++++++++++++++++")
    logger.info(f"Response status: {response.status}\n")
    if log_request_data:
        logger.info(f"Response data: {response.data}\n")

    return response


def encode_query_parameters_to_url(url, query_parameters):
    if query_parameters:
        encoded_url = url + "?" + urlencode(query_parameters)
        logger.info(f"Request URL with encoded query parameters= {encoded_url}")
        return encoded_url
    return url


def safe_json_loads(obj):
    try:
        return json.loads(obj)
    except json.decoder.JSONDecodeError:
        return obj


def get_secret(secret_id: str) -> dict:
    """
    Get secret values in Secrets Manager
    @param secret_id:
    @return: a dictionary containing client_id, client_secret, authorization_code, access_token and refresh token.
    """
    session = boto3.session.Session(region_name=os.environ["AWS_REGION"])
    client = session.client(service_name="secretsmanager", config=config)
    logger.info("Retrieving client id, client secret, refresh token, access token from Secrets Manager")
    try:
        res = client.get_secret_value(
            SecretId=secret_id,
        )
        return safe_json_loads(res["SecretString"])
    except Exception as e:
        logger.exception(
            "Failed to retrieve Client Id, Client Secret and Refresh Token from Secrets Manager.")
        logger.exception(e)


def validate_secrets(secrets: dict) -> None:
    """
    Validate client_id, client_secret, and refresh_token are not empty in Secrets Manager.
    @param secrets: a dictionary of secret values from Secrets Manager
    @return:
    """
    # Authorization_code is not used to make AMC API requests, so no need to validate it.
    # If the access_token is invalid or missing, a new access_token is retrieved using client_id, client_secret,refresh_token,
    # no need to validate access_token.
    if not (secrets.get("client_id") and secrets.get("client_secret") and secrets.get("refresh_token")):
        raise ValueError(
            "Client ID, Client Secret, and Refresh Token are required in Secrets Manager to make HTTP requests to Amazon Ads")


def update_secret(secret_id, secret_string):
    session = boto3.session.Session(region_name=os.environ["AWS_REGION"])
    client = session.client(service_name="secretsmanager", config=config)
    if isinstance(secret_string, dict):
        secret_string = json.dumps(secret_string)
    client.update_secret(SecretId=secret_id, SecretString=secret_string)


def get_access_token() -> dict:
    """
    Refresh an invalid or expired access token in Secrets Manager using client id, client secret and refresh token.
    @return: a dictionary of tokens.
    e.g. {"client_id": "XXX"
        "access_token": "XXX",
        "token_type": "bearer",
        "expires_in": 3600,
        "refresh_token": "XXXX"}
    """
    logger.info("Refresh access token using refresh token")
    secret_key = os.environ['AMC_SECRETS_MANAGER']

    secrets = get_secret(secret_key)
    validate_secrets(secrets)

    client_id = secrets["client_id"]
    client_secret = secrets["client_secret"]
    refresh_token = secrets["refresh_token"]

    code_payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    encoded_code_payload = json.dumps(code_payload)
    response = send_request(
        http_method="POST",
        request_url="https://api.amazon.com/auth/o2/token",
        headers=None,
        query_params={},
        data=encoded_code_payload,
    )

    # Throw an RuntimeError if access token is not retrieved successfully.
    # This RuntimeError will be captured and notify user via email.
    if response.status not in range(200, 204):
        raise RuntimeError(f"Cannot get access token. Response message: {response.data}")

    parsed_response = json.loads(response.data.decode('utf-8'))

    secret_value = {
        "client_id": client_id,
        "client_secret": client_secret,
        "authorization_code": secrets["authorization_code"],
        "refresh_token": refresh_token,
        "access_token": parsed_response["access_token"]
    }

    # Update Access Token in Secrets Manager.
    # The access token is retrieved successfully, exception in updating secrets doesn't affect Amazon Ads API calls,
    # so capture the exception, and resume API calls.
    try:
        update_secret(secret_key, secret_value)
    except Exception as ex:
        logger.exception(f"Cannot update access token in Secrets Manager. Reason: {ex}")

    return {"client_id": client_id, **parsed_response}


def verify_amc_request():
    clients_and_tokens = get_access_token()
    if clients_and_tokens.get("authorize_url"):
        raise RuntimeError("Unauthorized AMC request.")
    return clients_and_tokens


class WorkflowResponse:
    """Used to return a standard structured response if exception captured before making http requests to Amazon Ads"""

    def __init__(self, is_success, response_status, response_message):
        self.success = is_success or False
        self.response = {
            "responseStatus": response_status or "FAILED",
            'responseMessage': response_message or "Check state machine logs for more detail"
        }


class AMCAPIResponse:
    """Used to return a standard structured response by processing the response from the AMC API"""

    def __init__(self, response: urllib3.HTTPResponse):
        self.utils = wfm_utilities.Utils(logger)
        self.http_response = response
        self.response_data = json.loads(response.data.decode("utf-8"))
        self.status_code = response.status
        self.response_status = self.response_data.get('status') or str(self.status_code)
        self.response_received_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.response_text = response.data.decode("utf-8")

        logger.info("\nSTRUCTURED HTTP RESPONSE+++++++++++++++++++++++++++++++++++")

        # Information can be passed back differently by AMC depending on failure types, we want capture as much detail as possible
        for k in ['message', 'statusReason']:
            if self.response_data.get(k):
                self.response_message = self.response_data.get(k)
                break
            else:
                self.response_message = self.response_status

        self.response = {"responseReceivedTime": self.response_received_time,
                         "responseStatus": self.response_status, "statusCode": response.status,
                         "requestURL": response.geturl(), 'responseMessage': self.response_message}

        if self.utils.is_json(self.response_text):
            self.response.update(json.loads(self.response_text))

        if self.status_code in range(200, 204):
            self.success = True
            # Remove status from response and rename to responseStatus because status is a reserved word in DynamoDB
            if 'status' in self.response:
                del self.response['status']
        else:
            self.success = False
            self.response['responseStatus'] = 'FAILED'

        self.response_summary = f"Response Status: {self.response_status} | Response Message: {self.response_message} | Response Received Time: {self.response_received_time} | Response Url: {response.geturl()}"

    def update_response_status(self, status: str):
        self.response_status = status
        self.response['responseStatus'] = status

    def update_response(self, update: dict):
        self.response.update(update)

    def log_response_summary(self):
        if self.success:
            logger.info(self.response_summary)
        else:
            logger.error(self.response_summary)


class AMCRequests:
    def __init__(
            self,
            http_method,
            amc_path,
            payload=None,
            request_parameters=None,
    ) -> None:
        self.http_method = http_method.upper()
        self.amc_path = f"/{amc_path}".replace("//", "/")
        self.payload = payload or ""
        self.request_parameters = request_parameters or {}

    def process_request(self, kwargs) -> AMCAPIResponse:
        """
        Prepare url and headers then make HTTP requests.

        Parameters
        ----------
        kwargs : dict
            Dictionary containing AMC Instance ID, Advertiser ID, Marketplace ID, Client ID, and Access Token.

        Returns
        -------
        AMCAPIResponse:
            Structure Response from the Amazon Ads API.
        """
        amc_path = f"/amc/reporting/{kwargs['instance_id']}{self.amc_path}"
        base_url = "https://advertising-api.amazon.com/"
        request_url = urllib.parse.urljoin(base_url, amc_path)

        headers = {
            "Amazon-Advertising-API-ClientId": kwargs["client_id"],
            "Authorization": f'Bearer {kwargs["access_token"]}',
            "Content-Type": "application/json",
            "x-amzn-service-name": "amazon-marketing-cloud-insights-on-aws",
            "x-amzn-service-version": SOLUTION_VERSION
        }

        if kwargs.get("advertiser_id"):
            headers["Amazon-Advertising-API-AdvertiserId"] = kwargs[
                "advertiser_id"
            ]
        if kwargs.get("marketplace_id"):
            headers["Amazon-Advertising-API-MarketplaceId"] = kwargs[
                "marketplace_id"
            ]

        logger.debug(f"AMC_REQUEST_URL: {request_url}")
        logger.debug(f"AMC_REQUEST_PAYLOAD: {self.payload}")
        logger.debug(f"AMC_HTTP_METHOD: {self.http_method}")
        logger.debug(f"AMC_REQUEST_PARAMETERS: {self.request_parameters}")

        # Use client id and access token stored in Secrets Manager to authorize requests.
        # If requests are Unauthorized (status code 401), refresh the access token, then make request again using
        # the new access token.
        response = send_request(
            request_url=request_url,
            headers=headers,
            http_method=self.http_method,
            data=self.payload,
            query_params=self.request_parameters,
        )

        if response.status == 401:
            logger.info(
                f"Request to {request_url} is Unauthorized (status code 401), refresh access token, then try again")
            tokens = verify_amc_request()
            headers["Authorization"] = f'Bearer {tokens["access_token"]}'

            response = send_request(
                request_url=request_url,
                headers=headers,
                http_method=self.http_method,
                data=self.payload,
                query_params=self.request_parameters,
            )

        return AMCAPIResponse(response)


class AMCAPIs:
    """
    Used to interact with and AMC API Endpoint by making HTTP Requests For Workflow CRUD operations and
        Workflow Execute and Cancel requests
    """

    def __init__(
            self,
            customer_config: dict,
            wfm_utils: wfm_utilities.Utils,
    ):
        """
        Creates a new instance of the interface object based upon the configuration

        Parameters
        ----------
        customer_config
            Dictionary containing details about the AMC Instance that will be invoked
        """
        self.wfm_utils = wfm_utils
        self.customer_config = customer_config
        logger.info(f"Customer Id: {self.customer_config['customerId']}")

    def get_ads_parameters(self) -> dict:
        """
        Retrieve secret values from Secrets Manager, AMC Instance ID, Advertiser ID, Marketplace ID from Customer Config.
        Prepare url info and headers for HTTP requests.
        """
        secret_key = os.environ['AMC_SECRETS_MANAGER']
        secrets = get_secret(secret_key)
        validate_secrets(secrets)

        client_id = secrets.get("client_id")
        access_token = secrets.get("access_token", "")

        instance_id = self.customer_config["amcInstanceId"]

        if not (instance_id and self.customer_config.get("amcAmazonAdsMarketplaceId") and self.customer_config.get(
                "amcAmazonAdsAdvertiserId")):
            raise ValueError(
                f"AMC instances: instance_id, marketplace_id, and advertiser_id required for instance {instance_id}."
            )

        return {
            "client_id": client_id,
            "access_token": access_token,
            "instance_id": instance_id,
            "marketplace_id": self.customer_config["amcAmazonAdsMarketplaceId"],
            "advertiser_id": self.customer_config["amcAmazonAdsAdvertiserId"]
        }

    # returns the execution status for an execution based on execution id
    def get_execution_status_by_workflow_execution_id(
            self,
            workflow_execution_id
    ) -> Union[AMCAPIResponse, WorkflowResponse]:
        """
        Gets the status for one particular execution based on its `execution_id`

        Parameters
        ----------
        workflow_execution_id :
            guid string identifier for the execution to retrieve the status for

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API
        """
        try:
            ads_kwargs = self.get_ads_parameters()

            query_parameters = {
                "includeWorkflow": False
            }

            amc_request = AMCRequests(
                amc_path=f"/workflowExecutions/{workflow_execution_id}",
                http_method="GET",
                request_parameters=query_parameters,
            )

            amc_response = amc_request.process_request(ads_kwargs)

            amc_response.log_response_summary()
            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    # Returns all executions for the Endpoint created after a specified creation time in %Y-%m-%dT00:00:00 format
    def get_execution_status_by_minimum_create_time(
            self,
            min_creation_time: str = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%dT%H:%M:%S')
    ) -> Union[AMCAPIResponse, WorkflowResponse]:
        """
        Gets all executions that were created after the specified `minCreationTime`

        Parameters
        ----------
        min_creation_time :
            start date filter for executions to receive

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API
        """
        try:
            executions = []
            next_token = ''
            request_execution_status = True

            ads_kwargs = self.get_ads_parameters()

            while request_execution_status:
                query_parameters = {
                    "minCreationTime": min_creation_time,
                    "nextToken": next_token
                }

                amc_request = AMCRequests(
                    amc_path="/workflowExecutions",
                    http_method="GET",
                    request_parameters=query_parameters,
                )

                amc_response = amc_request.process_request(ads_kwargs)

                if amc_response.success:
                    executions += amc_response.response.get('executions').copy()

                    next_token = amc_response.response.get('nextToken', '')
                    if next_token == '':
                        request_execution_status = False

                else:
                    break

            amc_response.response['executions'] = executions

            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    def create_workflow(
            self,
            workflow_definition: dict,
            update_if_already_exists=True
    ) -> Union[AMCAPIResponse, WorkflowResponse]:
        """
        Create an AMC workflow based on the `workflow_definition` if the workflow already exists an error message will
        be displayed, but an update request will automatically be created if `update_if_already_exists` is true

        Parameters
        ----------
        workflow_definition:
            dictionary containing the workflow definition that will be sent to AMC. See AMC Documentation
            for the proper structure for a workflow
        update_if_already_exists:
            indicates if the workflow should be updated if it already exists

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API
        """
        try:
            workflow_id = workflow_definition.get('workflowId', '')
            request_body = json.dumps(
                workflow_definition, default=self.wfm_utils.json_encoder_default)

            ads_kwargs = self.get_ads_parameters()

            amc_request = AMCRequests(
                amc_path="/workflows",
                http_method="POST",
                payload=request_body,
            )

            amc_response = amc_request.process_request(ads_kwargs)

            if amc_response.success:
                amc_response.update_response_status('CREATED')
            elif (amc_response.response.get(
                    'responseMessage') == f"Workflow with ID {workflow_id} already exists.") and (
                    update_if_already_exists):
                logger.info(
                    amc_response.response.get('responseMessage') + ' Attempting Workflow Update on existing Workflow.')
                amc_response = self.update_workflow(
                    workflow_definition)

            amc_response.log_response_summary()
            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    def update_workflow(self, workflow_definition: dict) -> Union[AMCAPIResponse, WorkflowResponse]:
        """
        Updates an existing AMC workflow based on the `workflow_definition`

        Parameters
        ----------
        workflow_definition:
            dictionary containing the workflow definition that will be sent to AMC. See AMC Documentation
            for the proper structure for a workflow

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API
        """
        try:
            request_body = json.dumps(
                workflow_definition, default=self.wfm_utils.json_encoder_default)
            workflow_id = workflow_definition.get('workflowId', '')

            ads_kwargs = self.get_ads_parameters()

            amc_request = AMCRequests(
                amc_path=f"/workflows/{workflow_id}",
                http_method="PUT",
                payload=request_body,
            )

            amc_response = amc_request.process_request(ads_kwargs)

            if amc_response.success:
                amc_response.update_response_status('UPDATED')

            amc_response.log_response_summary()
            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    def delete_workflow(self, workflow_id: str) -> Union[AMCAPIResponse, WorkflowResponse]:
        """
        Deletes an existing AMC workflow based on the `workflow_id`

        Parameters
        ----------
        workflow_id :
            Identifier of the workflow

        Returns
        -------
        AMCAPIResponse or WorkflowResponse:
            Empty response from the AMC Endpoint API denoting successful deletion of a workflow.
        """
        try:
            amc_response = self.get_workflow(workflow_id)

            if amc_response.success:
                ads_kwargs = self.get_ads_parameters()

                amc_request = AMCRequests(
                    amc_path=f"/workflows/{workflow_id}",
                    http_method="DELETE",

                )

                amc_response = amc_request.process_request(ads_kwargs)

                if amc_response.success:
                    amc_response.update_response_status('DELETED')

            amc_response.log_response_summary()
            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    def get_workflow(self, workflow_id: str) -> Union[WorkflowResponse, AMCAPIResponse]:
        """
        Gets an existing AMC workflow based on the `workflow_definition` and returns the definition of the workflow

        Parameters
        ----------
        workflow_id:
            ID of the workflow to retrieve

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API containing the definition of the workflow that was deleted
        """
        try:
            ads_kwargs = self.get_ads_parameters()

            amc_request = AMCRequests(
                amc_path=f"/workflows/{workflow_id}",
                http_method="GET",
            )

            amc_response = amc_request.process_request(ads_kwargs)

            if amc_response.success:
                amc_response.update_response_status('RECEIVED')

            amc_response.log_response_summary()
            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    def get_workflows(self) -> Union[AMCAPIResponse, WorkflowResponse]:
        """
        Gets an all AMC workflows for the AMC instance

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API containing the definition of all workflows for the AMC instance

        TODO: This API is not used by WFM, review whether it's needed. If needed, implement nextToken.
        """
        try:
            ads_kwargs = self.get_ads_parameters()

            amc_request = AMCRequests(
                amc_path="/workflows",
                http_method="GET",
            )

            amc_response = amc_request.process_request(ads_kwargs)

            if amc_response.success:
                amc_response.update_response_status('RECEIVED')

            amc_response.log_response_summary()
            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    # Creates an AMC workflow execution, allows for dynamic date offsets like TODAY(-1) etc.
    def create_workflow_execution(self, create_execution_request: dict) -> Union[AMCAPIResponse, WorkflowResponse]:
        """
        Executes a workflow by creating a workflow execution specified in `create_execution_request`. This function
        will process the TimeWindowStart, TimeWindowEnd and custom parameter fields and replace their values if the
        value strings contain a predefined function such has NOW() TODAY() etc. see the process_parameter_functions
        method for more details on supported functions.

        Parameters
        ----------
        create_execution_request:
            A dictionary containing the create workflow request. See AMC Documentation for the proper structure for a
            workflow execution

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API containing the definition of all workflows for the AMC instance
        """
        try:
            # Process the parameters to enable now() and today() functions
            if "parameterValues" in create_execution_request:
                for parameter in create_execution_request['parameterValues']:
                    create_execution_request['parameterValues'][parameter] = self.wfm_utils.process_parameter_functions(
                        create_execution_request['parameterValues'][parameter])
                    logger.info(
                        "updated parameter {} to {}".format(parameter,
                                                            create_execution_request['parameterValues'][parameter]))
            if 'timeWindowStart' in create_execution_request:
                create_execution_request['timeWindowStart'] = self.wfm_utils.process_parameter_functions(
                    create_execution_request['timeWindowStart'])
                logger.info("updated parameter timeWindowStart to {}".format(
                    create_execution_request['timeWindowStart']))
            if 'timeWindowEnd' in create_execution_request:
                create_execution_request['timeWindowEnd'] = self.wfm_utils.process_parameter_functions(
                    create_execution_request['timeWindowEnd'])
                logger.info("updated parameter timeWindowEnd to {}".format(
                    create_execution_request['timeWindowEnd']))

            # Set up the HTTP Call
            request_body = json.dumps(create_execution_request)

            ads_kwargs = self.get_ads_parameters()

            amc_request = AMCRequests(
                amc_path="/workflowExecutions",
                http_method="POST",
                payload=request_body,
            )

            amc_response = amc_request.process_request(ads_kwargs)

            amc_response.log_response_summary()
            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))

    # returns the execution status for a execution based on execution id
    def cancel_workflow_execution(self, workflow_execution_id) -> Union[AMCAPIResponse, WorkflowResponse]:
        """
        Cancels an individual workflow execution specified by the `workflow_execution_id` by sending a DELETE request

        Parameters
        ----------
        workflow_execution_id:
            guid string identifier for the execution to be cancelled

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API containing the definition of all workflows for the AMC instance
        """
        try:
            amc_response = self.get_execution_status_by_workflow_execution_id(
                workflow_execution_id)
            logger.info(f"workflow_execution_id: {workflow_execution_id}")
            logger.info(f"Current workflow execution status: {amc_response}")

            # if the execution status was returned but is not CANCELLED or SUCCEEDED then make the delete call
            if (amc_response.success) and (amc_response.status_code not in ['CANCELLED', 'SUCCEEDED']):
                ads_kwargs = self.get_ads_parameters()

                request_body = json.dumps({"status": "CANCELLED"})

                amc_request = AMCRequests(
                    amc_path=f"/workflowExecutions/{workflow_execution_id}",
                    http_method="PUT",
                    payload=request_body,
                )

                amc_response = amc_request.process_request(ads_kwargs)

                # if the delete call was successful make a subsequent check to get the cancelled execution details
                if amc_response.success:
                    amc_response = self.get_execution_status_by_workflow_execution_id(
                        workflow_execution_id)
                    if amc_response.success and amc_response.response_status != 'CANCELLED':
                        amc_response.success = False
                        logger.info("Execution was not successfully cancelled")

            amc_response.log_response_summary()
            return amc_response
        except Exception as ex:
            logger.error(ex)
            # Return a structured response for state machine to notify users the request failed via email
            return WorkflowResponse(is_success=False, response_status="FAILED", response_message=repr(ex))
