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
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']

logger = LoggerUtil.create_logger()
send_metrics = metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger)
json_helper = JsonUtil()
dynamodb_helper = DynamodbHelper()
dynamic_date_evaluator = DynamicDateEvaluator()
map_helper = MapUtil()
client = get_service_client('stepfunctions')


def handler(event, _):
    logger.info(f"Event: {event}")
    send_metrics.put_metrics_count_value_1(metric_name="InvokeAdsReportSM")
    
    required_fields = ["profileId", "region", "requestBody"]
    for field in required_fields:
        if field not in event:
            logger.error(f"Missing required field: {field}")
            return json.dumps({"Invalid event data: profileId, region, and requestBody are required"})
        
    request_body = event['requestBody']
    profile_id = event['profileId']
    region = event['region']
    auth_id = event.get('authId', None)
        
    # We try to extract the report type from the request but fallback on a default value.
    # If the API needs it, this will fail during execution
    try:
        report_type_id = event['requestBody']['configuration']['reportTypeId']
        send_metrics.put_nested_metrics("AmazonAdsReporting-report_type", {report_type_id: 1})
    except KeyError as e:
        logger.error(f"Error extracting reportTypeId from requestBody: {e}")
        report_type_id = "default"
        
    # Customers can optionally pass in a custom table name to use downstream in Glue but will default to profileId-reportId if not provided.
    table_name = event.get('tableName', f'{profile_id}-{report_type_id}')
    if "/" in table_name: # table_name will be used as part of a specific s3 key prefix pattern - we do not allow this character
        logger.error(f"Invalid character '/' in table name: {table_name}")
        return json.dumps({"Invalid table name. Remove character '/'": str(table_name)})
        
    # Our data lake processing expects GZIP_JSON formatted files, so we override this parameter in our request
    configuration = event['requestBody'].get('configuration', {})
    configuration['format'] = 'GZIP_JSON'
    event['requestBody']['configuration'] = configuration
    
    # Check for custom date functions like TODAY() and LASTDAYOFOFFSETMONTH()
    map_helper.map_nested_dicts_modify(
        dict_to_process=event['requestBody'],
        function_to_apply=dynamic_date_evaluator.process_parameter_functions,
        date_format='%Y-%m-%d' # required format for amazon ads reporting
    )

    try:
        state_machine_input = {
            'tableName': table_name,
            'profileId': profile_id,
            'requestBody': request_body,
            'region': region,
            'authId': auth_id,
            'executionStateMachineArn': STEP_FUNCTION_STATE_MACHINE_ARN,
            'executionCreatedDate': datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
        }
        execution_name = f"{table_name}"[:65] + f"-{str(uuid.uuid4())[-10:]}"
        
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
