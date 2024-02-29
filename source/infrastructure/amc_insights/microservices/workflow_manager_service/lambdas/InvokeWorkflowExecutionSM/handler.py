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
from wfm_utilities import wfm_utilities
from cloudwatch_metrics import metrics

STEP_FUNCTION_STATE_MACHINE_ARN = os.environ['STEP_FUNCTION_STATE_MACHINE_ARN']
DATASET_WORKFLOW_TABLE = os.environ['DATASET_WORKFLOW_TABLE']
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']

# Create a logger instance and pass that to the common utils lambda_function layer class so it can log errors
logger = Logger(service="Workflow Management Service", level="INFO")
utils = wfm_utilities.Utils(logger)

client = get_service_client('stepfunctions')


def map_nested_dicts_modify(ob, func):
    for key, value in ob.items():
        if isinstance(value, Mapping):
            map_nested_dicts_modify(value, func)
        else:
            ob[key] = func(value)


def dynamodb_get_wf_config(dynamodb_table_name, customer_id, dataset_id):
    wf_config_item = {}
    # Set up a DynamoDB Connection
    dynamodb = get_service_client('dynamodb')

    # paginate in case there is large number of items
    paginator = dynamodb.get_paginator('query')
    response_iterator = paginator.paginate(
        TableName=dynamodb_table_name,
        Select='ALL_ATTRIBUTES',
        Limit=1,
        ConsistentRead=False,
        ReturnConsumedCapacity='TOTAL',
        KeyConditions={
            'customerId': {
                'AttributeValueList': [
                    {
                        'S': customer_id
                    },
                ],
                'ComparisonOperator': 'EQ'
            }
        }, PaginationConfig={'PageSize': 100}
    )

    # Iterate over each page from the iterator
    wf_config_list = []
    for page in response_iterator:
        # deserialize each "item" (or record) into a client config dictionary
        if 'Items' in page:
            for item in page['Items']:
                wf_config_item = utils.deserialize_dynamodb_item(item)
                # Run only "ACTIVE" jobs
                if wf_config_item['datasetId'] == dataset_id:
                    wf_config_list.append(wf_config_item)
    return wf_config_list


def handler(event, _):

    logger.info(f'event received:{event}')

    if event.get('Records'):
        msg = event['Records'][0]['Sns']['Message']
        parsed_message = json.loads(msg)

        if (
                parsed_message['uploadRequest']['requestType'] == 'uploadData'
                or parsed_message['uploadRequest']['requestType'] == 'uploadBulkData'
        ) and parsed_message['executionStatus'] == 'Upload Request Sent' \
                and parsed_message['responseStatus'] == 'Succeeded':

            logger.info(parsed_message)
            customer_id = parsed_message.get('customerId', 'demoCustomer')

            dataset_id = parsed_message.get("uploadRequest").get(
                'dataSetId',
                'samplefactdataset'
            )
            wf_config_list = dynamodb_get_wf_config(
                DATASET_WORKFLOW_TABLE,
                customer_id,
                dataset_id
            )

            if wf_config_list:
                wf_config_datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'
                start_state_machine(wf_config_list, wf_config_datetime_format, parsed_message, customer_id)
                return
            else:
                logger.info(
                    f"No workflow set to execute for dataset {dataset_id}")
                return
        else:
            print('DUS Response Failed or Is Not Data Upload - do not run workflow')
            return
    else:
        # read the customer config records
        customer_id = event.get('customerId', '')

        execution_request = copy.deepcopy(event)

    workflow_id = get_workflow_id(execution_request)

    execution_name = f"{customer_id}-{workflow_id}"[:65] + f"-{str(uuid.uuid4())[-10:]}"

    map_nested_dicts_modify(
        execution_request,
        utils.process_parameter_functions
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

    response = client.start_execution(
        stateMachineArn=STEP_FUNCTION_STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(state_machine_input, default=utils.json_encoder_default)
    )

    message = f"created state machine execution response : {response} for request {execution_request}"
    logger.info(message)

    # Record anonymized metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="InvokeWorkflowExecutionSM")

    return json.dumps(response, default=utils.json_encoder_default)


def get_config_details(wf_config, wf_config_datetime_format, parsed_message):
    config_start = datetime.strptime(
        wf_config['timeWindowStart'],
        wf_config_datetime_format
    ) if "timeWindowStart" in wf_config else None

    config_end = datetime.strptime(
        wf_config['timeWindowEnd']
        , wf_config_datetime_format
    ) if "timeWindowEnd" in wf_config else None

    dus_start = datetime.strptime(
        parsed_message['uploadRequest']['timeWindowStart'],
        '%Y-%m-%dT%H:%M:%SZ'
    ) if "timeWindowStart" in parsed_message['uploadRequest'] else None

    dus_end = datetime.strptime(
        parsed_message['uploadRequest']['timeWindowEnd'],
        '%Y-%m-%dT%H:%M:%SZ'
    ) if "timeWindowEnd" in parsed_message['uploadRequest'] else None

    return config_start, config_end, dus_start, dus_end


def get_workflow_id(execution_request):
    if execution_request['requestType'] == 'cancelExecution':
        workflow_id = 'cancel-workflow-execution'
    else:
        workflow_id = execution_request.get(
            "createExecutionRequest"
        ).get("workflowId")

    return workflow_id


def start_state_machine(wf_config_list, wf_config_datetime_format, parsed_message, customer_id):
    for wf_config in wf_config_list:
        logger.info(
            f'workflow config from {DATASET_WORKFLOW_TABLE}: {wf_config}'
        )

        config_start, config_end, dus_start, dus_end = get_config_details(wf_config, wf_config_datetime_format,
                                                                          parsed_message)

        try:
            time_window_start = min(
                filter(lambda d: d is not None, [config_start, dus_start])
            )
            time_window_end = max(
                filter(lambda d: d is not None, [config_end, dus_end])
            )

            execution_request = {
                "customerId": customer_id,
                "requestType": "createExecution",
                "createExecutionRequest": {
                    # "2021-06-02T00:00:00.000Z",
                    "timeWindowStart": time_window_start.strftime(wf_config_datetime_format),
                    # "2021-06-03T00:00:00.000Z",
                    "timeWindowEnd": time_window_end.strftime(wf_config_datetime_format),
                    "timeWindowType": "EXPLICIT",
                    "workflow_executed_date": "now()",
                    "timeWindowTimeZone": "America/New_York",
                    "workflowId": wf_config['workflowId'],
                    "ignoreDataGaps": True
                }
            }
            execution_name = f"{customer_id}-{wf_config['workflowId']}"[:65] + f"-{str(uuid.uuid4())[-10:]}"

            # Create a state machine input dictionary
            state_machine_input = {
                'initialWait': 5,
                'customerId': customer_id,
                'executionRequest': execution_request,
                'workflowExecutionName': execution_name,
                'executionStateMachineArn': STEP_FUNCTION_STATE_MACHINE_ARN,
                'executionCreatedDate': datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            }

            response = client.start_execution(
                stateMachineArn=STEP_FUNCTION_STATE_MACHINE_ARN,
                name=execution_name,
                input=json.dumps(
                    state_machine_input, default=utils.json_encoder_default)
            )

            message = f"created state machine execution response : {response} for request {execution_request}"
            logger.info(message)

        except Exception as e:
            logger.info(
                "Insufficient Time Window Start / End Information"
            )
            logger.info(e)
            response = "FAILED"
            continue
