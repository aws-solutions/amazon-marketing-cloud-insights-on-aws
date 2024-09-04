# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Mapping
import copy
import json
import os
import uuid
from datetime import datetime

from aws_solutions.core.helpers import get_service_client
from aws_lambda_powertools import Logger
from cloudwatch_metrics import metrics
from microservice_shared import utilities, dynamic_dates

STEP_FUNCTION_STATE_MACHINE_ARN = os.environ['STEP_FUNCTION_STATE_MACHINE_ARN']
DATASET_WORKFLOW_TABLE = os.environ['DATASET_WORKFLOW_TABLE']
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']

logger = Logger(service="Workflow Management Service", level="INFO")
json_helper = utilities.JsonUtil()
dynamic_date_evaluator = dynamic_dates.DynamicDateEvaluator()
map_helper = utilities.MapUtil()
client = get_service_client('stepfunctions')
            

def get_workflow_id(data):
    if "workflow" in data["createExecutionRequest"]:
        return data["createExecutionRequest"]["workflow"]["workflowId"]
    elif "workflowId" in data['createExecutionRequest']:
        return data["createExecutionRequest"]["workflowId"]
    else:
        raise KeyError


def handler(event, _):
    logger.info(f'event received:{event}')

    execution_request = copy.deepcopy(event)

    try:
        workflow_id = get_workflow_id(execution_request)
        customer_id = execution_request['customerId']
    except KeyError:
        msg = "workflowId and customerId are required in execution request"
        logger.error(msg)
        return json.dumps({"Input validation error:" : str(msg)})
    
    execution_name = f"{customer_id}-{workflow_id}"[:65] + f"-{str(uuid.uuid4())[-10:]}"

    map_helper.map_nested_dicts_modify(
        dict_to_process=execution_request,
        function_to_apply=dynamic_date_evaluator.process_parameter_functions
    )

    # Create a state machine input dictionary
    state_machine_input = {
        'initialWait': 5,
        'customerId': customer_id,
        'executionRequest': execution_request,
        'workflowExecutionName': execution_name,
        'executionStateMachineArn': STEP_FUNCTION_STATE_MACHINE_ARN,
        'executionCreatedDate': datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    }
    logger.info(f"state machine input: {state_machine_input}")

    response = client.start_execution(
        stateMachineArn=STEP_FUNCTION_STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(state_machine_input, default=json_helper.json_encoder_default)
    )

    message = f"created state machine execution response : {response} for request {execution_request}"
    logger.info(message)

    # Record anonymized metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="InvokeWorkflowExecutionSM")

    return json.dumps(response, default=json_helper.json_encoder_default)
