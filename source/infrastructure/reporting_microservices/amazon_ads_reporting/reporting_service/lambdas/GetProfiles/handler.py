# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from amazon_ads_api_interface import amazon_ads_api_interface
from microservice_shared.utilities import LoggerUtil
from cloudwatch_metrics import metrics

logger = LoggerUtil.create_logger()

METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
STACK_NAME = os.environ['STACK_NAME']
       
def handler(event, _) -> dict:
    """
    Lambda handler function to process an event containing region data and retrieve Amazon Ads profiles for each specified region.

    Parameters
    ----------
    event : dict
        The event data containing region information and optionally, authentication ID ('authId').
            {
                "region": ["North America", "Europe"],
                "authId": "example_auth_id"
            }

    Returns
    -------
    dict : A dictionary mapping each region to its respective profiles or an appropriate message if no profiles are found or an error occurs.
        {
            "North America": ["profile1", "profile2"],
            "Europe": "No profiles found"
        }
    """
    logger.info(f"Event: {event}")

    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="AdsGetProfiles")
    
    if "region" not in event:
        raise ValueError("Invalid event data: 'region' is required")
    
    region_list = event["region"]
    if not isinstance(region_list, list):
        region_list = [region_list]
        
    auth_id = event.get('authId', None)
    profile_map = {}
    
    for region in region_list:
        amazon_ads_reporting = amazon_ads_api_interface.AmazonAdsAPIs(
            region=region
            , auth_id=auth_id
        )
        response = amazon_ads_reporting.get_profiles_by_region()
        
        # successful response will return a list
        if isinstance(response, list):
            if not response: # if list is empty
                profile_map[region] = "No profiles found"
            profile_map[region] = response
        else:
            profile_map[region] = response.response["responseMessage"]
    
    logger.info(f"Profile map: {profile_map}")
    return profile_map    
    