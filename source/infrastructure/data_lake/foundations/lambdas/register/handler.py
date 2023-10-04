# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import os
from typing import Dict, Union

from crhelper import CfnResource
from aws_solutions.core.helpers import get_service_resource
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

OCTAGON_DATASET_TABLE_NAME = os.environ['OCTAGON_DATASET_TABLE_NAME']
OCTAGON_PIPELINE_TABLE_NAME = os.environ['OCTAGON_PIPELINE_TABLE_NAME']

logger = Logger(service="Registers Datasets Pipelines and Stages ", level="INFO", utc=True)
helper = CfnResource(log_level="ERROR", boto_level="ERROR")


def put_item(table_name: str, item: Dict[str, Union[str, int]]) -> None:
    dynamodb = get_service_resource("dynamodb")
    table = dynamodb.Table(table_name)
    try:
        table.put_item(Item=item)
    except ClientError as err:
        logger.error(err)
        raise err

def get_props(event):
    return event["ResourceProperties"]["RegisterProperties"]

def get_table_name(props):
    if 'dataset' in props['type']:
        return OCTAGON_DATASET_TABLE_NAME
    elif 'pipeline' in props['type']:
        return OCTAGON_PIPELINE_TABLE_NAME

def delete_item(table_name: str, key: Dict[str, str]) -> None:
    dynamodb = get_service_resource("dynamodb")
    table = dynamodb.Table(table_name)
    try:
        table.delete_item(Key=key)
    except ClientError as err:
        logger.error(err)
        raise err

@helper.create
def on_create(event, _) -> Dict[str, str]:
    props = get_props(event)
    table_name = get_table_name(props)
    physical_id = f"{props['id']}-ddb-item"
    try:
        put_item(table_name=table_name, item=props)
    except Exception as err:
        logger.error(err)
        raise err
    logger.info(f"Create resource {physical_id} with props {props}")
    return {"PhysicalResourceId": physical_id}

@helper.update
def on_update(event, _) -> Dict[str, str]:
    props = get_props(event)
    table_name = get_table_name(props)
    physical_id = event["PhysicalResourceId"]
    try:
        put_item(table_name=table_name, item=props)
    except Exception as err:
        logger.error(err)
        raise err
    logger.info(f"Update resource {physical_id} with props {props}")
    return {"PhysicalResourceId": physical_id}

@helper.delete
def on_delete(event, _) -> Dict[str, str]:
    props = get_props(event)
    table_name = get_table_name(props)
    physical_id = event["PhysicalResourceId"]
    if table_name in [OCTAGON_PIPELINE_TABLE_NAME, OCTAGON_DATASET_TABLE_NAME]:
        try:
            delete_item(table_name=table_name, key={"name": props["name"]})
        except Exception as err:
            logger.error(err)
            raise err
        logger.info(f"Delete resource {physical_id} with props {props}")
    else:
        try:
            delete_item(table_name=table_name, key={"id": props["id"]})
        except Exception as err:
            logger.error(err)
            raise err
        logger.info(f"Delete resource {physical_id} with props {props}")
    return {"PhysicalResourceId": physical_id}


def event_handler(event, context):
    """
    This is the Lambda custom resource entry point.
    """
    logger.info(event)
    
    props = event["ResourceProperties"]["RegisterProperties"]
    table_name = get_table_name(props)
    logger.info(f"Register in table {table_name}")

    event["ResourceProperties"]["RegisterProperties"]['version'] = int(props['version'])
    if "datasets" in props['type']:
        event["ResourceProperties"]["RegisterProperties"]['min_items_process']['stage_c'] = int(event["ResourceProperties"]["RegisterProperties"]['min_items_process']['stage_c'])
        event["ResourceProperties"]["RegisterProperties"]['min_items_process']['stage_b'] = int(event["ResourceProperties"]["RegisterProperties"]['min_items_process']['stage_b'])
        event["ResourceProperties"]["RegisterProperties"]['max_items_process']['stage_c'] = int(event["ResourceProperties"]["RegisterProperties"]['max_items_process']['stage_c'])
        event["ResourceProperties"]["RegisterProperties"]['max_items_process']['stage_b'] = int(event["ResourceProperties"]["RegisterProperties"]['max_items_process']['stage_b'])

    helper(event , context)