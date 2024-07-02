# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
from aws_lambda_powertools import Logger
# Import the common lambda_function layer functions
from wfm_amc_api_interface import wfm_amc_api_interface
from wfm_utilities import wfm_utilities
from cloudwatch_metrics import metrics


# Create a logger instance and pass that to the common utils lambda_function layer class so it can log errors
logger = Logger(service="Workflow Management Service", level="INFO")

utils = wfm_utilities.Utils(logger)
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
AMC_SECRETS_MANAGER = os.environ['AMC_SECRETS_MANAGER']


def handler(event, context):
    """
    Retrieve access and refresh tokens from AMC using client id, client secret, and authorization code stored in Secrets Manager.
    """

    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="AMCAuth")
    
    event['EXECUTION_RUNNING_LAMBDA_NAME'] = context.function_name

    secrets = wfm_amc_api_interface.get_secret(AMC_SECRETS_MANAGER)

    if not (secrets.get("client_id") and secrets.get("client_secret") and secrets.get("authorization_code")):
        raise ValueError(
            f"Client ID, Client Secret, and Authorization Code are required in Secrets Manager {AMC_SECRETS_MANAGER} to make authorization request to Amazon Ads")

    client_id = secrets["client_id"]
    client_secret = secrets["client_secret"]
    authorization_code = secrets["authorization_code"]

    auth_payload = {
        "grant_type": "authorization_code",
        "code": authorization_code,
        "redirect_uri": "https://amazon.com",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    encoded_code_payload = json.dumps(auth_payload)
    response = wfm_amc_api_interface.send_request(
        http_method="POST",
        request_url="https://api.amazon.com/auth/o2/token",
        headers=None,
        query_params={},
        data=encoded_code_payload,
        log_request_data=False
    )

    # Throw an RuntimeError if auth tokens are not retrieved successfully.
    if response.status not in range(200, 204):
        raise RuntimeError(f"Cannot retrieve authorization tokens. Response message: {response.data}")

    parsed_response = json.loads(response.data.decode('utf-8'))

    secret_value = {
        "client_id": client_id,
        "client_secret": client_secret,
        "authorization_code": secrets["authorization_code"],
        "refresh_token": parsed_response["refresh_token"],
        "access_token": parsed_response["access_token"]
    }

    # Update authorization tokens in Secrets Manager.
    try:
        wfm_amc_api_interface.update_secret(AMC_SECRETS_MANAGER, secret_value)
    except Exception as ex:
        logger.exception(f"Cannot update authorization tokens in Secret Manager {AMC_SECRETS_MANAGER}. Reason: {ex}")

    return event
