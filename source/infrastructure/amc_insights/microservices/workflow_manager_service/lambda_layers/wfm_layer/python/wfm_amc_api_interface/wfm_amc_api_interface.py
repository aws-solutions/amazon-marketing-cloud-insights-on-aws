# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from typing import Union
from datetime import datetime, timedelta
import urllib3
from botocore import config
import urllib

from wfm_utilities import wfm_utilities
from microservice_shared.secrets import SecretsHelper
from microservice_shared.utilities import JsonUtil, LoggerUtil
from microservice_shared.api import ApiHelper, RequestParams
from microservice_shared.dynamic_dates import DynamicDateEvaluator


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
dynamic_date_evaluator = DynamicDateEvaluator()


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
        self.http_response = response
        self.response_data = json.loads(response.data.decode("utf-8"))
        self.status_code = response.status
        self.response_status = self.response_data.get('status') or str(self.status_code)
        self.response_received_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.response_text = response.data.decode("utf-8")

        logger.info("\nSTRUCTURED HTTP RESPONSE+++++++++++++++++++++++++++++++++++")

        # Information can be passed back differently by AMC depending on failure types, we want capture as much detail as possible
        for k in ['message', 'statusReason', 'details', 'messageSubject']:
            if self.response_data.get(k):
                self.response_message = self.response_data.get(k)
                break
            else:
                self.response_message = self.response_status

        self.response = {"responseReceivedTime": self.response_received_time,
                         "responseStatus": self.response_status, "statusCode": response.status,
                         "requestURL": response.geturl(), 'responseMessage': self.response_message}

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
        
        self.secrets_helper = SecretsHelper(
            secret_key=os.environ["AMC_SECRETS_MANAGER"], 
            auth_id=self.customer_config.get("authId", None)
        )
        
    def verify_amc_request(self) -> dict:
        clients_and_tokens = self.secrets_helper.get_access_token()
        if clients_and_tokens.get("authorize_url"):
            raise RuntimeError("Unauthorized AMC request.")
        return clients_and_tokens
    
    def process_request(self, amc_request, kwargs) -> AMCAPIResponse:
        """
        Prepare url and headers then make HTTP requests.

        Parameters
        ----------
        amc_request : RequestParams
            RequestParams object containing the necessary parameters for the AMC API request.
        kwargs : dict
            Dictionary containing AMC Instance ID, Advertiser ID, Marketplace ID, Client ID, and Access Token.

        Returns
        -------
        AMCAPIResponse:
            Structure Response from the Amazon Ads API.
        """
        amc_path = f"/amc/reporting/{kwargs['instance_id']}{amc_request.request_path}"
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
        logger.debug(f"AMC_REQUEST_PAYLOAD: {amc_request.payload}")
        logger.debug(f"AMC_HTTP_METHOD: {amc_request.http_method}")
        logger.debug(f"AMC_REQUEST_PARAMETERS: {amc_request.request_parameters}")

        # Use client id and access token stored in Secrets Manager to authorize requests.
        # If requests are Unauthorized (status code 401), refresh the access token, then make request again using
        # the new access token.
        response = api_helper.send_request(
            request_url=request_url,
            headers=headers,
            http_method=amc_request.http_method,
            data=amc_request.payload,
            query_params=amc_request.request_parameters,
        )

        if response.status == 401:
            logger.info(
                f"Request to {request_url} is Unauthorized (status code 401), refresh access token, then try again")
            tokens = self.verify_amc_request()
            headers["Authorization"] = f'Bearer {tokens["access_token"]}'

            response = api_helper.send_request(
                request_url=request_url,
                headers=headers,
                http_method=amc_request.http_method,
                data=amc_request.payload,
                query_params=amc_request.request_parameters,
            )

        return AMCAPIResponse(response)

    def get_ads_parameters(self) -> dict:
        """
        Retrieve secret values from Secrets Manager, AMC Instance ID, Advertiser ID, Marketplace ID from Customer Config.
        Prepare url info and headers for HTTP requests.
        """
        secrets = self.secrets_helper.get_secret()
        self.secrets_helper.validate_secrets(secrets)

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

            amc_request = RequestParams(
                request_path=f"/workflowExecutions/{workflow_execution_id}",
                http_method="GET",
                request_parameters=query_parameters,
            )

            amc_response = self.process_request(amc_request, ads_kwargs)

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

                amc_request = RequestParams(
                    request_path="/workflowExecutions",
                    http_method="GET",
                    request_parameters=query_parameters,
                )

                amc_response = self.process_request(amc_request, ads_kwargs)

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
                workflow_definition, default=json_helper.json_encoder_default)

            ads_kwargs = self.get_ads_parameters()

            amc_request = RequestParams(
                request_path="/workflows",
                http_method="POST",
                payload=request_body,
            )

            amc_response = self.process_request(amc_request, ads_kwargs)

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
                workflow_definition, default=json_helper.json_encoder_default)
            workflow_id = workflow_definition.get('workflowId', '')

            ads_kwargs = self.get_ads_parameters()

            amc_request = RequestParams(
                request_path=f"/workflows/{workflow_id}",
                http_method="PUT",
                payload=request_body,
            )

            amc_response = self.process_request(amc_request, ads_kwargs)

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

                amc_request = RequestParams(
                    request_path=f"/workflows/{workflow_id}",
                    http_method="DELETE",

                )

                amc_response = self.process_request(amc_request, ads_kwargs)

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

            amc_request = RequestParams(
                request_path=f"/workflows/{workflow_id}",
                http_method="GET",
            )

            amc_response = self.process_request(amc_request, ads_kwargs)

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

            amc_request = RequestParams(
                request_path="/workflows",
                http_method="GET",
            )

            amc_response = self.process_request(amc_request, ads_kwargs)

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
                    create_execution_request['parameterValues'][parameter] = dynamic_date_evaluator.process_parameter_functions(
                        create_execution_request['parameterValues'][parameter])
                    logger.info(
                        "updated parameter {} to {}".format(parameter,
                                                            create_execution_request['parameterValues'][parameter]))
            if 'timeWindowStart' in create_execution_request:
                create_execution_request['timeWindowStart'] = dynamic_date_evaluator.process_parameter_functions(
                    create_execution_request['timeWindowStart'])
                logger.info("updated parameter timeWindowStart to {}".format(
                    create_execution_request['timeWindowStart']))
            if 'timeWindowEnd' in create_execution_request:
                create_execution_request['timeWindowEnd'] = dynamic_date_evaluator.process_parameter_functions(
                    create_execution_request['timeWindowEnd'])
                logger.info("updated parameter timeWindowEnd to {}".format(
                    create_execution_request['timeWindowEnd']))

            # Set up the HTTP Call
            request_body = json.dumps(create_execution_request)

            ads_kwargs = self.get_ads_parameters()

            amc_request = RequestParams(
                request_path="/workflowExecutions",
                http_method="POST",
                payload=request_body,
            )

            amc_response = self.process_request(amc_request, ads_kwargs)

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

                amc_request = RequestParams(
                    request_path=f"/workflowExecutions/{workflow_execution_id}",
                    http_method="PUT",
                    payload=request_body,
                )

                amc_response = self.process_request(amc_request, ads_kwargs)

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
