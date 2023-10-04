# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from aws_solutions.core.helpers import get_service_client
import os
from aws_lambda_powertools import Logger
from patterns import TpsDeployPatterns
from cloudwatch_metrics import metrics

TEMPLATE_URL = os.environ["TEMPLATE_URL"]
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
STACK_NAME = os.environ['RESOURCE_PREFIX']

logger = Logger(service="Tenant Provisioning Service", level="INFO")
s3 = get_service_client("s3")
kms = get_service_client("kms")


def launch_stack(event):
    if (event.get('TenantName') != None and event.get('BucketName') and event.get('amcOrangeAwsAccount')):
        logger.info('Initiate the TpsDeployPatterns class')

        tps_deploy_patterns = TpsDeployPatterns(event)
        tps_deploy_patterns, customer_type = check_tps_deploy_patterns(tps_deploy_patterns)

        stack_name = tps_deploy_patterns.stack_name
        template_params = tps_deploy_patterns.template_params
        template_pattern = tps_deploy_patterns.template_pattern
        template_url = TEMPLATE_URL
        template_url = template_url + "/" + template_pattern

        logger.info(f'template_url: {template_url}')

        if customer_type == "standard":
            stack_resp = tps_deploy_patterns.deploy_stack(
                stack_name=stack_name,
                template_url=template_url,
                parameters=template_params,
                region=tps_deploy_patterns._bucket_region
            )

        elif customer_type == "cross-account":
            stack_resp = tps_deploy_patterns.deploy_stack(
                stack_name=stack_name,
                template_url=template_url,
                parameters=template_params,
                region=tps_deploy_patterns._application_region
            )

            if tps_deploy_patterns._data_lake_enabled == "Yes":
                data_lake_template_obj = tps_deploy_patterns.create_cross_account_data_lake_template()
                upload_template(
                    template=data_lake_template_obj.template_string,
                    bucket_name=tps_deploy_patterns._artifacts_bucket_name,
                    file_name=f"{tps_deploy_patterns._bucket_account}/data-lake/cross-account-data-lake-{tps_deploy_patterns._event['TenantName']}.json"
                )
                update_bucket_policy(
                    bucket_name=tps_deploy_patterns._artifacts_bucket_name,
                    account=tps_deploy_patterns._bucket_account,
                    artifacts_bucket=tps_deploy_patterns._artifacts_bucket_name
                )
                update_key_policy(
                    key_id=tps_deploy_patterns._artifacts_bucket_key_id,
                    account_id=tps_deploy_patterns._bucket_account
                )

        elif customer_type == "cross-region":
            tps_deploy_patterns.deploy_stack(
                stack_name=stack_name,
                template_url=template_url,
                parameters=template_params,
                region=tps_deploy_patterns._application_region
            )

            base_params = template_params.copy()
            add_params = [
                {
                    'ParameterKey': 'pBucketExists',
                    'ParameterValue': tps_deploy_patterns._bucket_exists,
                },
                {
                    'ParameterKey': 'pSkipCrossRegionEvents',
                    'ParameterValue': 'false',
                },
                {
                    'ParameterKey': 'pSkipAPIRoles',
                    'ParameterValue': 'true',
                },
                {
                    'ParameterKey': 'pSkipDatalakeTrigger',
                    'ParameterValue': 'true',
                },
                {
                    'ParameterKey': 'pSkipSnsTopic',
                    'ParameterValue': 'true'
                }
            ]
            base_params.extend(add_params)
            stack_resp = tps_deploy_patterns.deploy_stack(
                stack_name=f"{stack_name}-crossregion",
                template_url=template_url,
                parameters=base_params,
                region=tps_deploy_patterns._bucket_region
            )

        if tps_deploy_patterns._red_room_account != tps_deploy_patterns._application_account:
            microservice_template_obj = tps_deploy_patterns.create_cross_account_wfm_template()
            upload_template(
                template=microservice_template_obj.template_string,
                bucket_name=tps_deploy_patterns._artifacts_bucket_name,
                file_name=f"{tps_deploy_patterns._red_room_account}/wfm/cross-account-wfm-{tps_deploy_patterns._event['TenantName']}.json"
            )
            update_bucket_policy(
                bucket_name=tps_deploy_patterns._artifacts_bucket_name,
                account=tps_deploy_patterns._red_room_account,
                artifacts_bucket=tps_deploy_patterns._artifacts_bucket_name
            )
            update_key_policy(
                key_id=tps_deploy_patterns._artifacts_bucket_key_id,
                account_id=tps_deploy_patterns._red_room_account
            )

        return stack_resp

    else:
        logger.info('Nothing to create/update. Missing input parameters')
        return 'no activity'


def check_tps_deploy_patterns(tps_deploy_patterns):
    # Check if data lake is enabled
    if tps_deploy_patterns.check_data_lake():
        tps_deploy_patterns.get_data_lake_params()

    # Check if creating new SNS topic for the customer
    if tps_deploy_patterns.check_sns_create():
        tps_deploy_patterns.template_params.extend(tps_deploy_patterns.sns_create_params)

        # Check if cross-account deployment
    if tps_deploy_patterns.check_cross_account():
        logger.info("Cross account customer detected")
        customer_type = "cross-account"
        tps_deploy_patterns.template_params.extend(tps_deploy_patterns.cross_account_params)
    # Check if cross-region deployment
    elif tps_deploy_patterns.check_cross_region():
        logger.info("Cross region customer detected")
        customer_type = "cross-region"
    else:
        logger.info("Standard customer detected")
        customer_type = "standard"
        tps_deploy_patterns.template_params.extend(tps_deploy_patterns.bucket_status_params)

    return tps_deploy_patterns, customer_type


def upload_template(template, bucket_name, file_name):
    obj_key = f"tps/scripts/adtech/scripts/cross-account/{file_name}"

    s3.put_object(
        Body=template,
        Bucket=bucket_name,
        Key=obj_key
    )


def update_bucket_policy(bucket_name, account, artifacts_bucket):
    statement_new = {
        'Sid': f'cross-account-{account}',
        'Effect': 'Allow',
        'Principal': {
            'AWS': [
                f"arn:aws:iam::{account}:root"
            ]
        },
        'Action': ['s3:GetObject'],
        'Resource': [
            f'arn:aws:s3:::{artifacts_bucket}/tps/scripts/adtech/scripts/cross-account/{account}/wfm/*',
            f'arn:aws:s3:::{artifacts_bucket}/tps/scripts/adtech/scripts/cross-account/{account}/data-lake/*'
        ]
    }

    try:
        response = s3.get_bucket_policy(Bucket=bucket_name)
        raw_policy = response['Policy']
        load_policy = json.loads(raw_policy)

        for statement in load_policy['Statement']:
            if 'Sid' in statement.keys():
                if statement['Sid'] == f'cross-account-{account}':
                    # no action needed
                    return
        # add new statement
        load_policy['Statement'].append(statement_new)
        formatted_policy = json.dumps(load_policy)
        s3.put_bucket_policy(Bucket=bucket_name, Policy=formatted_policy)

    except Exception:
        load_policy = {
            'Id': 'artifacts-bucket-policy',
            'Version': '2012-10-17',
            'Statement': []
        }
        # add new policy
        load_policy['Statement'].append(statement_new)
        formatted_policy = json.dumps(load_policy)
        s3.put_bucket_policy(Bucket=bucket_name, Policy=formatted_policy)


def update_key_policy(key_id, account_id):
    arn = f"arn:aws:iam::{account_id}:root"
    statement_new = {
        "Sid": "cross-account",
        "Effect": "Allow",
        "Principal": {
            "AWS": [
                arn
            ]
        },
        "Action": [
            "kms:Decrypt*"
        ],
        "Resource": "*"
    }

    response = kms.get_key_policy(
        KeyId=key_id,
        PolicyName='default'
    )
    raw_policy = response['Policy']
    load_policy = json.loads(raw_policy)

    for statement in load_policy['Statement']:
        if ('Sid' in statement.keys()
                and statement['Sid'] == 'cross-account'
        ):
            if arn in statement['Principal']['AWS'] or arn == statement['Principal']['AWS']:
                # no action needed
                return
            else:
                # add new statement arn
                current_principals = statement['Principal']['AWS']
                if type(current_principals) == list:
                    statement['Principal']['AWS'].append(arn)
                else:
                    statement['Principal']['AWS'] = [current_principals, arn]
                formatted_policy = json.dumps(load_policy)
                kms.put_key_policy(
                    KeyId=key_id,
                    PolicyName='default',
                    Policy=formatted_policy
                )
                return
    # add new statement
    load_policy['Statement'].append(statement_new)
    formatted_policy = json.dumps(load_policy)
    kms.put_key_policy(
        KeyId=key_id,
        PolicyName='default',
        Policy=formatted_policy
    )


def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="AddAMCInstance")

    logger.info('event: {}'.format(json.dumps(event, indent=2)))
    stack_resp = launch_stack(event)

    return stack_resp
