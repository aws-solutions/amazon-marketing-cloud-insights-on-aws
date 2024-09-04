# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from datetime import datetime
import json
import uuid

from microservice_shared.utilities import LoggerUtil, JsonUtil, MapUtil
from microservice_shared.dynamic_dates import DynamicDateEvaluator
from microservice_shared.dynamodb import DynamodbHelper
from aws_solutions.core.helpers import get_service_client
from cloudwatch_metrics import metrics

STEP_FUNCTION_STATE_MACHINE_ARN = os.environ['STEP_FUNCTION_STATE_MACHINE_ARN']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
STACK_NAME = os.environ['STACK_NAME']

logger = LoggerUtil.create_logger()
send_metrics = metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger)
json_helper = JsonUtil()
dynamodb_helper = DynamodbHelper()
dynamic_date_evaluator = DynamicDateEvaluator()
map_helper = MapUtil()
client = get_service_client('stepfunctions')


def handler(event, _):
    """
    AWS Lambda function to start an AWS Step Functions state machine execution for Sp Reporting.

    This function validates the input `event`, processes custom date functions in the request body,
    and starts the state machine execution using the provided parameters. If the execution is
    successfully started, the response from Step Functions is returned.

    :param event: The event data passed to the Lambda function, containing details required for
                  starting the state machine execution. Must include 'region' and 'requestBody'.
    :param _: The context parameter (not used in this function).

    :return: A JSON-encoded response from Step Functions if the execution is successful,
             or an error message if any required fields are missing or if the execution fails.
    """
    logger.info(f"Event: {event}")
    send_metrics.put_metrics_count_value_1(metric_name="InvokeSpReportSM")
    
    required_fields = ["region", "requestBody"]
    for field in required_fields:
        if field not in event:
            logger.error(f"Missing required field: {field}")
            return json.dumps({"Invalid event data: region, and requestBody are required"})
        
    request_body = event['requestBody']
    region = event['region']
    auth_id = event.get('authId', None)
        
    # We try to extract the report type from the request but fallback on a default value.
    # If the API needs it, this will fail during execution
    try:
        report_type = event['requestBody']['reportType']
        send_metrics.put_nested_metrics("SellingPartnerReporting-report_type", {report_type: 1})
    except KeyError as e:
        logger.error(f"Error extracting reportType from requestBody: {e}")
        report_type = "default"
        
    # Customers can optionally pass in a custom table prefix to use downstream in Glue but will default to region-reportType if not provided.
    default_prefix = f"{region}-{report_type}".replace(" ", "")
    table_prefix = event.get('tablePrefix', default_prefix)
    if "/" in table_prefix: # table_prefix will be used as part of a specific s3 key prefix pattern - we do not allow this character
        logger.error(f"Invalid character '/' in table prefix: {table_prefix}")
        return json.dumps({"Invalid table prefix. Remove character '/'": str(table_prefix)})
    
    # Check for custom date functions like TODAY() and LASTDAYOFOFFSETMONTH()
    map_helper.map_nested_dicts_modify(
        dict_to_process=event['requestBody'],
        function_to_apply=dynamic_date_evaluator.process_parameter_functions,
        date_format='%Y-%m-%d' # required format for selling partner reporting
    )

    try:
        state_machine_input = {
            'tablePrefix': table_prefix,
            'requestBody': request_body,
            'region': region,
            'authId': auth_id,
            'executionStateMachineArn': STEP_FUNCTION_STATE_MACHINE_ARN,
            'executionCreatedDate': datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
        }
        execution_name = f"{table_prefix}"[:65] + f"-{str(uuid.uuid4())[-10:]}"
        
        logger.info(f"Starting state machine execution with input {state_machine_input}")
        response = client.start_execution(
            stateMachineArn=STEP_FUNCTION_STATE_MACHINE_ARN,
            name=execution_name,
            input=json.dumps(state_machine_input, default=json_helper.json_encoder_default)
        )

        logger.info(f"State machine execution response : {response}")
        return json.dumps(response, default=json_helper.json_encoder_default)
    
    except Exception as e:
        logger.error(f"Error starting state machine: {e}")
        return json.dumps({"Error starting state machine": str(e)})
