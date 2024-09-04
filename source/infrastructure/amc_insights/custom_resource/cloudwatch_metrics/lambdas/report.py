# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#   This function reads Cloudwatch metrics from a prescribed namespace and
#   reports that data, aggregated over the past 24 hours, to the solution
#   builder metrics endpoint. This function should be scheduled to run once per
#   day.
###############################################################################
import os
import json
import logging
import requests
from datetime import datetime, timedelta
from aws_solutions.core.helpers import get_service_client

SOLUTION_ID = os.environ["SOLUTION_ID"]
SOLUTION_VERSION = os.environ["SOLUTION_VERSION"]
METRICS_NAMESPACE = os.environ["METRICS_NAMESPACE"]
SEND_ANONYMIZED_DATA = os.environ["SEND_ANONYMIZED_DATA"]

SECONDS_IN_A_DAY = 86400

# Format log messages like this:
formatter = logging.Formatter(
    "{%(pathname)s:%(lineno)d} %(levelname)s - %(message)s"
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
# Clear the default logger before attaching the custom logging handler
# in order to avoid duplicating each log message:
logging.getLogger().handlers.clear()
# Attach the custom logging handler:
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

METRICS_ENDPOINT = 'https://metrics.awssolutionsbuilder.com/generic'

# These metrics return numerical values that can be summed directly
METRICS_TO_SUM = [
    # Lambda Metrics
    'AddAMCInstance',
    'AddAMCInstanceCheck',
    'AddAMCInstancePostDeployMetadata',
    'CancelWorkflowExecution',
    'CheckWorkflowExecutionStatus',
    'CreateWorkflow',
    'CreateWorkflowExecution',
    'CreateWorkflowSchedule',
    'DatalakeRouting',
    'DeleteWorkflow',
    'DeleteWorkflowSchedule',
    'GetExecutionSummary',
    'GetWorkflow',
    'SdlfHeavyTransformCheckJob',
    'SdlfHeavyTransformError',
    'SdlfHeavyTransformPostupdateMetadata',
    'SdlfHeavyTransformProcessObject',
    'SdlfHeavyTransformRedrive',
    'SdlfHeavyTransformRouting',
    'UpdateWorkflow',
    'ScheduleAdsReport',
    'ScheduleSpReport',
    # State machine metrics:
    'InvokeTPSInitializeSM',
    'InvokeWorkflowExecutionSM',
    'InvokeWorkflowSM',
    'InvokeAdsReportSM',
    'SdlfHeavyTransformRedriveSM',
    'SdlfHeavyTransformRoutingSM',
    'SdlfLightTransformSM',
    # Glue Job metrics:
    'SdlfHeavyTransformJob-num_files',
    'SdlfHeavyTransformJob-bytes_read',
    'SdlfHeavyTransformJob-bytes_written',
    'SdlfHeavyTransformJob-num_records',
    'SdlfHeavyTransformJob-run_count',
    # AmazonAdsReport
    'AdsCheckReportStatus',
    'AdsDownloadReport',
    'AdsGetProfiles',
    'InvokeAdsReportSM',
    'RequestSponsoredAdsReport',
    # SellingPartnerReport
    'SellersPartnerGetReportDocument',
    'InvokeSpReportSM',
    'SellerPartnerCreateReport',
    'SellersPartnerDownloadReport',
    'SellersPartnerGetReportStatus'
]

# These metrics return nested values that we need to unpack before summing
NESTED_METRICS = [
    'AmazonAdsReporting-report_type',
    'SellingPartnerReporting-report_type',
]

def event_handler(event, context):
    """
    This function is the entry point.
    """
    logger.info("We got the following event:\n")
    logger.info("event:\n {s}".format(s=event))
    logger.info("context:\n {s}".format(s=context))
    if SEND_ANONYMIZED_DATA == "Yes":
        logger.info("Report anonymized operational metrics")
        send_metrics()
    else:
        logger.info("Anonymized data collection is opted out, no operational metrics to report")

def update_nested_metrics(data, metric_name, datapoints):
    for datapoint in datapoints:
        for key, _ in datapoint.items():
            if key != 'Sum':
                update_metric_sum(data, metric_name, key, datapoint['Sum'])

def update_metric_sum(data, metric_name, key, sum_value):
    if key in data["Data"].get(metric_name, {}):
        data["Data"][metric_name][key] += sum_value
    else:
        data["Data"].setdefault(metric_name, {})[key] = sum_value

def handle_metrics(data, metric_name, datapoints):
    if metric_name in NESTED_METRICS:
        update_nested_metrics(data, metric_name, datapoints)
    else:
        if len(datapoints) > 1:
            logging.warning("Got " + str(
                len(datapoints)) + " datapoints but only expected one datapoint since period is one day and start/end time spans one day.")
        total = 0
        for datapoint in datapoints:
            # There should only be one datapoint since period is one day and
            # start/end time spans one day, but if there is more than one datapoint
            # then sum them together:
            total += datapoint["Sum"]
        # Add the sum to the reporting payload:
        data["Data"][metric_name] = total

def send_metrics():
    """
    This function is responsible for reporting cloudwatch metrics.
    """
    cloudwatch_client = get_service_client('cloudwatch')
    secrets_manager_client = get_service_client("secretsmanager")
    # Setup metric query fields:
    end_time = datetime.utcnow()
    start_time = (end_time - timedelta(seconds=SECONDS_IN_A_DAY))
    uuid = secrets_manager_client.get_secret_value(
        SecretId=f"{os.environ['STACK_NAME']}-anonymized-metrics-uuid"
    )['SecretString']
    # Assemble the payload structure for reporting:
    data = {
        "Solution": SOLUTION_ID,
        "Version": SOLUTION_VERSION,
        "UUID": uuid,
        "TimeStamp": str(datetime.now()),
        "Data": {}
    }
    
    all_metrics = NESTED_METRICS + METRICS_TO_SUM
    for metric_name in all_metrics:
        response = cloudwatch_client.get_metric_statistics(
            Namespace=METRICS_NAMESPACE,
            MetricName=metric_name,
            StartTime=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            EndTime=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            Period=SECONDS_IN_A_DAY,
            Statistics=['Sum'],
            Dimensions=[
                {'Name': 'stack-name', 'Value': os.environ['STACK_NAME']}
            ]
        )
        datapoints = response.get('Datapoints', [])
        
        if datapoints:
            handle_metrics(data, metric_name, datapoints)

    if data["Data"]:
        logging.info("Reporting the following data:")
        logging.info(json.dumps(data))
        response = requests.post(METRICS_ENDPOINT, json=data, timeout=5)
        logger.info(f"Response status code = {response.status_code}")
    else:
        logging.info("No data to report.")
