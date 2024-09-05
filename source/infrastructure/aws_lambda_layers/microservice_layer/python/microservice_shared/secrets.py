# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json

from aws_solutions.core.helpers import get_service_client
from microservice_shared.utilities import LoggerUtil, JsonUtil
from microservice_shared.api import ApiHelper

class SecretsHelper:
    """
    Helper class for managing secrets stored in AWS Secrets Manager.
    """
    def __init__(self, secret_key, auth_id=None):
        """
        Initializes the SecretsHelper instance.

        Parameters
        ----------
        secret_key : str
            The key identifying the secret in AWS Secrets Manager.
        auth_id : str, optional
            The authorization Id key for the token credentials.
        """
        self.secret_key = secret_key
        self.auth_id = auth_id
        self.secret_string = {} #initialized as empty dict and populated by get_secret() method
        self.logger = LoggerUtil.create_logger()
        self.client = get_service_client(service_name="secretsmanager", region_name=os.environ["AWS_REGION"])
        self.api_helper = ApiHelper()
        
        # to maintain backwards compatability with single-cred deployment, we set auth_id as optional
        if auth_id:
            self.logger.info(f"SecretsHelper initialized for {self.secret_key} with Auth Id: {self.auth_id}")
        else:
            self.logger.info(f"SecretsHelper initialized for {self.secret_key} without Auth Id")
        
    def validate_secrets(self, secrets: dict) -> None:
        """
        Validate that client_id, client_secret, and refresh_token are not empty in Secrets Manager.

        Parameters
        ----------
        secrets : dict
            A dictionary of secret values from Secrets Manager.
        
        Raises
        ------
        ValueError
            If any of client_id, client_secret, or refresh_token are missing.
        """
        # Authorization_code is not used to make AMC API requests, so no need to validate it.
        # If the access_token is invalid or missing, a new access_token is retrieved using client_id, client_secret, refresh_token,
        # no need to validate access_token.
        if not (secrets.get("client_id") and secrets.get("client_secret") and secrets.get("refresh_token")):
            raise ValueError(
                f"""Client ID, Client Secret, and Refresh Token are required in Secrets Manager {self.secret_key} to make HTTP requests to Amazon Ads.
                    If configured with multiple credentials ensure Auth Id is included in the request.
                """)
    
    def get_secret(self) -> dict:
        """
        Retrieve secret values from Secrets Manager.

        Returns
        -------
        dict
            A dictionary containing client_id, client_secret, authorization_code, access_token, and refresh token.
        
        Raises
        ------
        Exception
            If there is an error retrieving the secrets.
        """
        self.logger.info(f"Retrieving client id, client secret, refresh token, access token from Secret Manager {self.secret_key}")
        try:
            res = self.client.get_secret_value(
                SecretId=self.secret_key,
            )
            self.secret_string = JsonUtil.safe_json_loads(res["SecretString"])
            if self.auth_id:
                credentials = self.secret_string[self.auth_id]
            else:
                credentials = self.secret_string
            return credentials
        except Exception as e:
            self.logger.exception(
                f"Failed to retrieve Client Id, Client Secret and Refresh Token from Secrets Manager {self.secret_key}.")
            self.logger.exception(e)

    def update_secret(self, secret_value):
        """
        Update secret values in Secrets Manager.

        Parameters
        ----------
        secret_value : str or dict
            The new credential values to update.
        """
        if self.auth_id:
            self.secret_string[self.auth_id] = secret_value
        else:
            self.secret_string = secret_value
        self.client.update_secret(SecretId=self.secret_key, SecretString=json.dumps(self.secret_string))

    def get_access_token(self) -> dict:
        """
        Refresh an invalid or expired access token in Secrets Manager using client_id, client_secret, and refresh_token.

        Returns
        -------
        dict
            A dictionary containing the new tokens, e.g. {"client_id": "XXX", "access_token": "XXX", "token_type": "bearer", "expires_in": 3600, "refresh_token": "XXXX"}
        
        Raises
        ------
        RuntimeError
            If the access token is not retrieved successfully.
        """
        self.logger.info("Refresh access token using refresh token")
        self.logger.debug(f"secret_key: {self.secret_key}")

        secrets = self.get_secret()
        self.validate_secrets(secrets)

        client_id = secrets["client_id"]
        client_secret = secrets["client_secret"]
        refresh_token = secrets["refresh_token"]

        code_payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }

        encoded_code_payload = json.dumps(code_payload)
        response = self.api_helper.send_request(
            http_method="POST",
            request_url="https://api.amazon.com/auth/o2/token",
            headers=None,
            query_params={},
            data=encoded_code_payload,
            log_request_data=False,
        )

        # Throw a RuntimeError if access token is not retrieved successfully.
        # This RuntimeError will be captured and notify the user via email.
        if response.status not in range(200, 204):
            raise RuntimeError(f"Cannot get access token. Response message: {response.data}")

        parsed_response = json.loads(response.data.decode('utf-8'))

        updated_secret_value = {
            "client_id": client_id,
            "client_secret": client_secret,
            "authorization_code": secrets.get("authorization_code", ""),
            "refresh_token": refresh_token,
            "access_token": parsed_response["access_token"]
        }

        # Update Access Token in Secrets Manager.
        # The access token is retrieved successfully, exception in updating secrets doesn't affect Amazon Ads API calls,
        # so capture the exception, and resume API calls.
        try:
            self.update_secret(updated_secret_value)
        except Exception as ex:
            self.logger.exception(f"Cannot update access token in Secret Manager {self.secret_key}. Reason: {ex}")

        return {"client_id": client_id, **parsed_response}
