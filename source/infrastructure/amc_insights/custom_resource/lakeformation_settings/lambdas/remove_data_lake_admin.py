# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import time
from crhelper import CfnResource
from aws_lambda_powertools import Logger
from aws_solutions.core.helpers import get_service_client

logger = Logger(service='Remove deployed roles from Lake Formation administrator list', level="INFO")
helper = CfnResource()

lakeformation_client = get_service_client("lakeformation")

MAX_RETRIES = 3

def lakeformation_admins(role_list):
    response = lakeformation_client.get_data_lake_settings()
    data_lake_settings = response['DataLakeSettings']
    datalake_admins = data_lake_settings['DataLakeAdmins']

    return_admins = []
    if datalake_admins:
        for item in datalake_admins:
            if item['DataLakePrincipalIdentifier'] not in role_list:
                return_admins.append(item)

        data_lake_settings['DataLakeAdmins'] = return_admins
        lakeformation_client.put_data_lake_settings(DataLakeSettings=data_lake_settings)
        logger.info("Lake Formation admins removed")
    
    else:
        logger.info("No Lake Formation admins found. Nothing to action")

@helper.delete
def on_delete(event, _):
    logger.info(f"Delete event input: {json.dumps(event)}")
    ADMIN_ROLE_LIST = event["ResourceProperties"]['ADMIN_ROLE_LIST']

    retry = True
    attempts = 0
    while retry:
        attempts += 1
        if attempts > MAX_RETRIES:
            logger.error(f"Unable to edit Lake Formation admins. Remove {ADMIN_ROLE_LIST} manually from the console")
            retry = False
        else:
            try:
                lakeformation_admins(role_list=ADMIN_ROLE_LIST)
                retry = False
            except lakeformation_client.exceptions.ConcurrentModificationException:
                logger.info("Concurrent modification exception. Waiting 10 seconds to retry")
                time.sleep(10)
            except Exception as e:
                logger.error(f"Unable to edit Lake Formation admins: {e}. Remove {ADMIN_ROLE_LIST} manually from the console")
                retry = False
            
    
@helper.create
@helper.update
def create_update(event, _):
    logger.info(f"Create/Update event input: {event}. Nothing to action")
    

def event_handler(event, context):
    """
    This function is the entry point for the Lambda-backed custom resource.
    """
    logger.info(event)
    try:
        helper(event, context)
    except Exception as e:
        logger.error(e)
    # We always send a 'Success' message back to cloudformation, as errors
    # in this custom resource should not cause the stack to fail deletion
    # but simply raise the error message back so users can take action 
    # manually in the Lake Formation console.
    helper.send_response(event, 'SUCCESS')
