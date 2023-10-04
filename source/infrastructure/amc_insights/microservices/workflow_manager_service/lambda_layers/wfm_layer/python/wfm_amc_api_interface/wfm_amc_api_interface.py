# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta
from urllib.parse import urlencode
from aws_solutions.core.helpers import get_service_client
import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import boto3

from wfm_utilities import wfm_utilities


temp = '{"AMCInsights-ServiceName":"WFM", "x-amzn-service-name":"amci-wfm", "x-amzn-service-version": "2.0.0"}'
additional_headers = json.loads(os.getenv(temp, '{}'))

class AMCAPIResponse:
    """used to return a standard structured response by processing the response from the AMC API"""

    def __init__(self, logger, utils, response: urllib3.HTTPResponse):
        self.logger = logger
        self.utils = utils
        self.http_response = response
        self.response_data = json.loads(response.data.decode("utf-8"))
        self.status_code = response.status
        self.response_status = self.response_data['status'] if 'status' in self.response_data else str(
            self.status_code)
        self.response_received_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.response_text = response.data.decode("utf-8")
        # information can be passed back differently by AMC depending on failure types, we want capture as much detail as possible
        for k in ['message', 'statusReason']:
            if self.response_data.get(k):
                self.response_message = self.response_data.get(k)
                break
            else:
                self.response_message = self.response_status
        self.response = {"responseReceivedTime": self.response_received_time,
                         "responseStatus": self.response_status, "statusCode": response.status, "requestURL": response.geturl(), 'responseMessage': self.response_message}
        
        if self.utils.is_json(self.response_text):
            self.response.update(json.loads(self.response_text))
        if self.status_code in range(200, 204):
            self.success = True
            # remove status from response and rename to responseStatus because status is a reserved word in DynamoDB
            if 'status' in self.response:
                del self.response['status']

        else:
            self.success = False
            self.response['responseStatus'] = 'FAILED'

        self.response_summary = f"Response Status: {self.response_status} | Response Message: {self.response_message} | Response Recieved Time: {self.response_received_time} | Response Url: {response.geturl()}"

    def update_response_status(self, status: str):
        self.response_status = status
        self.response['responseStatus'] = status

    def update_response(self, update: dict):
        self.response.update(update)

    def log_response_summary(self):
        if self.success:
            self.logger.info(self.response_summary)
        else:
            self.logger.error(self.response_summary)

# This class will create HTTP Requests for AMC API Endpoints
class AMCAPIInterface:
    """
    Used to interact with and AMC API Endpoint by making HTTP Requests For Workflow CRUD operations and
        Workflow Execute and Cancel requests
    """

    def __init__(
        self, 
        config: dict, 
        logger: logging.Logger,
        utils: wfm_utilities.Utils, 
        ):
        """
        Creates a new instance of the interface object based upon the configuration

        Parameters
        ----------
        config
            Dictionary containing details about the AMC Endpoint that will be invoked
        logger
            logger object for the class to use log info and error events
        boto3_session
            the Boto3 Session to use to assume the invokeAMCAPI role which will invoke the endpoint
        """
        self.utils = utils
        self.logger = logger
        self.config = config

        # Set up the default boto3 session to be the customer specific InvokeAMCAPI Role
        logger.info('Starting session with customer role arn: {}'.format(self.config['invokeAmcApiRoleArn']))
        self.boto3_get_session_for_role(
            customer_role_arn = self.config['invokeAmcApiRoleArn'],
            )

    def boto3_get_session_for_role(
        self, 
        customer_role_arn
        ) -> bool:

        try:
            role_name = re.match("arn\:aws\:iam\:\:\d*\:role\/(.*)", customer_role_arn).groups()[0]
            session_name = f"{role_name[:51]}-{str(uuid.uuid4())[-12:]}"

            sts_connection = get_service_client('sts')
            customer_role_creds = sts_connection.assume_role(
                    RoleArn=customer_role_arn,
                    RoleSessionName=session_name
                )

            customer_boto3_session = boto3.Session(
                aws_access_key_id=customer_role_creds['Credentials']['AccessKeyId'],
                aws_secret_access_key=customer_role_creds['Credentials']['SecretAccessKey'],
                aws_session_token=customer_role_creds['Credentials']['SessionToken'],
            )
            self._boto3_session = customer_boto3_session
            self.logger.info('Successfully started boto session')

        except Exception as e:
            self.logger.error(e)
            return False

    # This function is used to generate headers for Sigv4 to authenticate with the AMC API Endpoint
    def apigateway_get_signed_headers(
        self, 
        request_method, 
        request_endpoint_url, 
        request_body,
        region
        ):

        self.logger.info("Signing headers for api request")

        # Generate signed http headers for Sigv4
        request = AWSRequest(
            method=request_method.upper(), 
            url=request_endpoint_url, 
            data=request_body
            )

        SigV4Auth(
            self._boto3_session.get_credentials(), 
            "execute-api",
            region
            ).add_auth(request)

        self.logger.info(f"Successfully signed headers {request.headers.items()}")
        return dict(request.headers.items())


    def send_amc_api_request(self, request_method, url, request_body='') -> AMCAPIResponse:
        """
        Sends an authenticated HTTP request to the AMC Endpoint using the boto3 session supplied

        Parameters
        ----------
        request_method : str
            GET|PUT|POST|DELETE
        url : str
            The URL of the endpoint to send the http request to
        request_body : str
            Body to include in the HTTP request

        Returns
        -------
        AMCAPIResponse
            A response object from the HTTP Call
        """

        signed_headers = self.apigateway_get_signed_headers(
            request_method,
            url,
            request_body,
            region=self.config['amcRegion']
        )

        headers = copy.deepcopy(signed_headers)

        pool_manager = urllib3.PoolManager()

        self.logger.info(f'Sending API request to AMC. Method: {request_method} | Url : {url} | Body: {request_body}')

        response: urllib3.HTTPResponse = pool_manager.request(
            request_method,
            url,
            headers=headers,
            body=request_body)

        return AMCAPIResponse(logger=self.logger, utils=self.utils, response=response)

    # returns the execution status for a execution based on execution id
    def get_execution_status_by_workflow_id(self, workflow_id) -> AMCAPIResponse:
        """
        Gets all workflow executions for a workflow based on `workflow_id` from the AMC Endpoint

        Parameters
        ----------
        workflowId :
            The ID for the workflow to return executions for

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API
        """

        url = f"{self.config['amcApiEndpoint']}/?workflowId={workflow_id}/"
        request_method = 'GET'
        request_body = ''

        amc_response = self.send_amc_api_request(
            request_method, url, request_body)

        amc_response.log_response_summary()
        return amc_response

    # returns the execution status for a execution based on execution id
    def get_execution_status_by_workflow_execution_id(self, workflow_execution_id) -> AMCAPIResponse:
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

        url = f"{self.config['amcApiEndpoint']}/workflowExecutions/{workflow_execution_id}"
        request_method = 'GET'
        request_body = ''

        amc_response = self.send_amc_api_request(
            request_method, url, request_body)

        amc_response.log_response_summary()
        return amc_response

    # Returns all executions for the Endpoint created after a specified creation time in %Y-%m-%dT00:00:00 format
    def get_execution_status_by_minimum_create_time(self, min_creation_time: str = (
            datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%dT%H:%M:%S')) -> AMCAPIResponse:
        """
        Gets all executions that were created after the specified `minCreationTime`

        Parameters
        ----------
        minCreationTime :
            start date filter for executions to receive

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API
        """

        executions = []
        next_token = ''
        request_execution_status = True

        while request_execution_status:

            url_query_values = urlencode(
                {'minCreationTime': min_creation_time, "nextToken": next_token})
            url = f"{self.config['amcApiEndpoint']}/workflowExecutions/?{url_query_values}"
            request_method = 'GET'
            request_body = ''

            amc_response = self.send_amc_api_request(
                request_method, url, request_body)

            if amc_response.success:
                executions += amc_response.response.get('executions').copy()

                next_token = amc_response.response.get('nextToken', '')
                if next_token == '':
                    request_execution_status = False

            else:
                break

        amc_response.response['executions'] = executions

        return amc_response

    def create_workflow(self, workflow_definition: dict, update_if_already_exists=True) -> AMCAPIResponse:
        """
        Create a AMC workflow based on the `workflow_definition` if the workflow already exists an error message will
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
        url = f"{self.config['amcApiEndpoint']}/workflows"
        request_method = 'POST'
        request_body = json.dumps(
            workflow_definition, default=self.utils.json_encoder_default)
        workflow_id = workflow_definition.get('workflowId', '')

        amc_response = self.send_amc_api_request(
            request_method, url, request_body)

        if amc_response.success:
            amc_response.update_response_status('CREATED')
        elif (amc_response.response.get('responseMessage') == f"Workflow with ID {workflow_id} already exists.") and (update_if_already_exists):
            self.logger.info(amc_response.response.get('responseMessage') + ' Attempting Workflow Update on existing Workflow.')
            amc_response = self.update_workflow(
                workflow_definition)

        amc_response.log_response_summary()
        return amc_response

    def update_workflow(self, workflow_definition: dict) -> AMCAPIResponse:
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

        request_body = json.dumps(
            workflow_definition, default=self.utils.json_encoder_default)
        workflow_id = workflow_definition.get('workflowId', '')

        url = f"{self.config['amcApiEndpoint']}/workflows/{workflow_id}"
        request_method = 'PUT'

        amc_response = self.send_amc_api_request(
            request_method, url, request_body)

        if amc_response.success:
            amc_response.update_response_status('UPDATED')

        amc_response.log_response_summary()
        return amc_response

    def delete_workflow(self, workflow_id: str) -> AMCAPIResponse:
        """
        Deletes an existing AMC workflow based on the `workflow_definition` and returns the definition of the workflow that was deleted

        Parameters
        ----------
        workflow_definition:
            dictionary containing the workflow definition that will be sent to AMC. See AMC Documentation
            for the proper structure for a workflow

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API containing the definition of the workflow that was deleted
        """

        amc_response = self.get_workflow(workflow_id)

        if amc_response.success:
            url = f"{self.config['amcApiEndpoint']}/workflows/{workflow_id}"
            request_method = 'DELETE'
            request_body = ''
            amc_response = self.send_amc_api_request(
                request_method, url, request_body)

            if amc_response.success:
                amc_response.update_response_status('DELETED')

        amc_response.log_response_summary()
        return amc_response

    def get_workflow(self, workflow_id: str) -> AMCAPIResponse:
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

        url = f"{self.config['amcApiEndpoint']}/workflows/{workflow_id}"
        request_method = 'GET'
        request_body = ''

        amc_response = self.send_amc_api_request(
            request_method, url, request_body)

        if amc_response.success:
            amc_response.update_response_status('RECEIVED')

        amc_response.log_response_summary()
        return amc_response

    def get_workflows(self) -> AMCAPIResponse:
        """
        Gets an all AMC workflows for the AMC instance

        Returns
        -------
        AMCAPIResponse:
            Response from the AMC Endpoint API containing the definition of all workflows for the AMC instance
        """

        url = f"{self.config['amcApiEndpoint']}/workflows"
        request_method = 'GET'
        request_body = ''

        amc_response = self.send_amc_api_request(
            request_method, url, request_body)

        if amc_response.success:
            amc_response.update_response_status('RECEIVED')
        
        amc_response.log_response_summary()
        return amc_response

    # Creates a AMC workflow execution, allows for dynamic date offsets like TODAY(-1) etc.
    def create_workflow_execution(self, create_execution_request: dict) -> AMCAPIResponse:
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

        # Process the parameters to enable now() and today() functions
        if "parameterValues" in create_execution_request:
            for parameter in create_execution_request['parameterValues']:
                create_execution_request['parameterValues'][parameter] = self.utils.process_parameter_functions(
                    create_execution_request['parameterValues'][parameter])
                self.logger.info(
                    "updated parameter {} to {}".format(parameter,
                                                        create_execution_request['parameterValues'][parameter]))
        if 'timeWindowStart' in create_execution_request:
            create_execution_request['timeWindowStart'] = self.utils.process_parameter_functions(
                create_execution_request['timeWindowStart'])
            self.logger.info("updated parameter timeWindowStart to {}".format(
                create_execution_request['timeWindowStart']))
        if 'timeWindowEnd' in create_execution_request:
            create_execution_request['timeWindowEnd'] = self.utils.process_parameter_functions(
                create_execution_request['timeWindowEnd'])
            self.logger.info("updated parameter timeWindowEnd to {}".format(
                create_execution_request['timeWindowEnd']))

        # Set up the HTTP Call
        url = f"{self.config['amcApiEndpoint']}/workflowExecutions"
        request_method = 'POST'
        request_body = json.dumps(create_execution_request)

        amc_response = self.send_amc_api_request(
            request_method, url, request_body)

        amc_response.log_response_summary()
        return amc_response

    # returns the execution status for a execution based on execution id
    def cancel_workflow_execution(self, workflow_execution_id) -> AMCAPIResponse:
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

        amc_response = self.get_execution_status_by_workflow_execution_id(
            workflow_execution_id)
        
        # if the execution status was returned but is not CANCELLED or SUCCEEDED then make the delete call
        if (amc_response.success) and (amc_response.status_code not in ['CANCELLED', 'SUCCEEDED']):
            url = f"{self.config['amcApiEndpoint']}/workflowExecutions/{workflow_execution_id}"
            request_method = 'DELETE'
            request_body = ''
            amc_response = self.send_amc_api_request(
                request_method, url, request_body)
            # if the delete call was successful make a subsequent check to get the cancelled execution details
            if amc_response.success:
                amc_response = self.get_execution_status_by_workflow_execution_id(
                    workflow_execution_id)
                if amc_response.success and amc_response.response_status != 'CANCELLED':
                    amc_response.success = False
                    self.logger("Execution was not successfully cancelled")
            
        amc_response.log_response_summary()
        return amc_response

