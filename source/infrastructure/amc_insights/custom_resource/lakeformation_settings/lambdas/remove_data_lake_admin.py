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

def lakeformation_admins(role_list):
    response = lakeformation_client.get_data_lake_settings()
    data_lake_settings = response['DataLakeSettings']
    datalake_admins = data_lake_settings['DataLakeAdmins']

    return_admins = []
    if len(datalake_admins) > 0:
        for item in datalake_admins:
            if item['DataLakePrincipalIdentifier'] not in role_list:
                return_admins.append(item)

    data_lake_settings['DataLakeAdmins'] = return_admins
    response = lakeformation_client.put_data_lake_settings(DataLakeSettings=data_lake_settings)
    logger.info(f"Request response: {response['ResponseMetadata']['HTTPStatusCode']}")

@helper.delete
def on_delete(event, _):
    logger.info(f"Delete event input: {json.dumps(event)}")
    ADMIN_ROLE_LIST = event["ResourceProperties"]['ADMIN_ROLE_LIST']

    retry = True
    attempts = 0
    while retry:
        attempts += 1
        if attempts > 2:
            logger.error(f"Unable to edit lakeformation admins. Remove {ADMIN_ROLE_LIST} manually from the console")
        else:
            try:
                lakeformation_admins(role_list=ADMIN_ROLE_LIST)
                retry = False
            except lakeformation_client.exceptions.ConcurrentModificationException:
                logger.info("Concurrent modification excpetion. Waiting 10 seconds to retry")
                time.sleep(10)
            except Exception as e:
                logger.error(e)
                raise e
            
    
@helper.create
@helper.update
def create_update(event, _):
    logger.info(f"Create event input: {json.dumps(event)}. Nothing to action.")
    

def event_handler(event, context):
    """
    This function is the entry point for the Lambda-backed custom resource.
    """
    logger.info(event)
    helper(event, context)

