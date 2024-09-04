# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
from aws_lambda_powertools import Logger

# Import the common lambda_function layer functions
from cloudwatch_metrics import metrics
from microservice_shared.secrets import SecretsHelper
from microservice_shared.api import ApiHelper


METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
AMC_SECRETS_MANAGER = os.environ['AMC_SECRETS_MANAGER']

logger = Logger(service="Workflow Management Service", level="INFO")
api_helper = ApiHelper()


def handler(event, _):
    """
    Retrieve access and refresh tokens from AMC using client id, client secret, and authorization code stored in Secrets Manager.
    """

    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="AMCAuth")
    
    # if multi-account credentials are set up, customers will pass in "auth_id" to identify which credentials to be used
    auth_id = event.get("auth_id", None)
    secrets_helper = SecretsHelper(AMC_SECRETS_MANAGER, auth_id=auth_id)

    secrets = secrets_helper.get_secret()

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
    response = api_helper.send_request(
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
        secrets_helper.update_secret(secret_value)
    except Exception as ex:
        logger.exception(f"Cannot update authorization tokens in Secret Manager {AMC_SECRETS_MANAGER}. Reason: {ex}")

    return event
