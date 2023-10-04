# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import boto3
import json
import re
import sys

resource_prefix = str(sys.argv[1])
profile_name = str(sys.argv[2])

session = boto3.session.Session(profile_name=profile_name)

s3_client = session.client('s3')
dynamodb_client = session.client('dynamodb')
kms_client = session.client('kms')
sqs_client = session.client("sqs")
lambda_client = session.client("lambda")
events_client = session.client('events')
cfn_client = session.client('cloudformation')
cw_client = session.client('logs')
lake_formation_client = boto3.client('lakeformation')

MAX_ITEMS = 1000


def list_s3_buckets(resource_prefix):
    buckets = []
    response = s3_client.list_buckets()
    for bucket in response["Buckets"]:
        if re.match(f"{resource_prefix}-*", bucket["Name"]):
            buckets.append(bucket["Name"])
    return buckets


def list_dynamodb_tables(resource_prefix):
    table_list = []
    response = dynamodb_client.list_tables()
    for table_name in response["TableNames"]:
        if re.match(f"{resource_prefix}-*", table_name) or re.match(f"octagon-*-{resource_prefix}", table_name):
            table_list.append(table_name)
    return table_list


def list_kms_keys(max_items, resource_prefix):
    key_id_list = []
    try:
        # creating paginator object for list_keys() method
        paginator = kms_client.get_paginator('list_aliases')

        # creating a PageIterator from the paginator
        response_iterator = paginator.paginate(
            PaginationConfig={'MaxItems': max_items})

        full_result = response_iterator.build_full_result()
        for page in full_result['Aliases']:
            if re.match(f"alias/{resource_prefix}-*", page["AliasName"]):
                response = kms_client.describe_key(
                    KeyId=page["TargetKeyId"]
                )
                if response["KeyMetadata"]["KeyState"] not in ["Disabled", "PendingDeletion", "Unavailable"]:
                    key_id_list.append(page["TargetKeyId"])
    except:
        print('Could not list KMS Keys.')
        raise
    else:
        return key_id_list


def list_sqs_queues(resource_prefix):
    queue_list = []
    response = sqs_client.list_queues(
        QueueNamePrefix=f"{resource_prefix}-"
    )
    if "QueueUrls" in response:
        queue_urls = response["QueueUrls"]
        for queue in queue_urls:
            queue_list.append(queue)
    return queue_list


def list_lambda_layers(resource_prefix):
    layer_list = []
    response = lambda_client.list_layers()
    for layer in response["Layers"]:
        if re.match("PowertoolsLayer*", layer["LayerName"]) or \
                re.match("data-lake-library", layer["LayerName"]) or \
                re.match("SolutionsLayer*", layer["LayerName"]) or \
                re.match(f"{resource_prefix}-wfm-*", layer["LayerName"]) or \
                re.match("AWSDataWrangler*", layer["LayerName"]):
            layer_list.append(
                {
                    "layerName": layer["LayerName"],
                    "version": layer["LatestMatchingVersion"]["Version"]
                }
            )
    return layer_list


def list_rules(resource_prefix):
    rule_list = []
    response = events_client.list_rules(
        NamePrefix=f'{resource_prefix}-'
    )
    for rule in response["Rules"]:
        rule_list.append(rule["Name"])
    return rule_list


def list_cfn_template(resource_prefix):
    stack_list = []
    response = cfn_client.list_stacks(
        StackStatusFilter=['CREATE_COMPLETE', 'ROLLBACK_COMPLETE', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE']
    )
    for stack in response["StackSummaries"]:
        if re.match(f"{resource_prefix}-[a-zA-Z0-9_.-]*-instance-[a-zA-Z0-9_.-]*", stack["StackName"]):
            stack_list.append(stack["StackName"])
        else:
            continue
    return stack_list


def list_cw_logs(resource_prefix):
    cw_log_list = []
    try:
        # creating paginator object for describe_log_groups() method
        paginator = cw_client.get_paginator('describe_log_groups')

        # creating a PageIterator from the paginator
        response_iterator = paginator.paginate(
            PaginationConfig={'MaxItems': MAX_ITEMS})

        full_result = response_iterator.build_full_result()
        for page in full_result['logGroups']:
            if re.match(f"/aws/lambda/{resource_prefix}-*", page["logGroupName"]):
                cw_log_list.append(page["logGroupName"])
    except:
        print('Could not list CloudWatch logs.')
        raise
    else:
        return cw_log_list


def list_lake_formation_settings(resource_prefix):
    response = lake_formation_client.get_data_lake_settings()
    datalake_settings = response['DataLakeSettings']

    try:
        datalake_admins_to_keep = []
        for admin in datalake_settings['DataLakeAdmins']:
            admin_prefix, admin_name = admin['DataLakePrincipalIdentifier'].split("/")
            if not admin_name.startswith(resource_prefix):
                datalake_admins_to_keep.append(
                    {'DataLakePrincipalIdentifier': '/'.join([admin_prefix, admin_name])}
                )

        database_default_permissions_to_keep = [permission for permission in
                                                datalake_settings['CreateDatabaseDefaultPermissions'] if
                                                permission['Principal'] in datalake_admins_to_keep]

        table_default_permissions_to_keep = [permission for permission in
                                             datalake_settings['CreateTableDefaultPermissions']
                                             if permission['Principal'] in datalake_admins_to_keep]

        external_data_filter_allow_list_to_keep = [identifier for identifier in
                                                   datalake_settings['ExternalDataFilteringAllowList'] if
                                                   identifier in datalake_admins_to_keep]
        datalake_settings_to_keep = {
            'DataLakeAdmins': datalake_admins_to_keep,
            'CreateDatabaseDefaultPermissions': database_default_permissions_to_keep,
            'CreateTableDefaultPermissions': table_default_permissions_to_keep,
            'Parameters': datalake_settings['Parameters'],
            'TrustedResourceOwners': datalake_settings['TrustedResourceOwners'],
            'AllowExternalDataFiltering': datalake_settings['AllowExternalDataFiltering'],
            'ExternalDataFilteringAllowList': external_data_filter_allow_list_to_keep,
            'AuthorizedSessionTagValueList': datalake_settings['AuthorizedSessionTagValueList']
        }
        return datalake_settings_to_keep
    except Exception as err:
        print(f"Could not list data lake settings to keep, error: {err}")




if __name__ == "__main__":
    try:
        print("Collecting resources to delete")
        resources = {}

        resources["s3"] = list_s3_buckets(resource_prefix)

        resources["dynamodb"] = list_dynamodb_tables(resource_prefix)

        resources["kms"] = list_kms_keys(MAX_ITEMS, resource_prefix)

        resources["sqs"] = list_sqs_queues(resource_prefix)

        resources["lambda_layer"] = list_lambda_layers(resource_prefix)

        resources["event_bridge"] = list_rules(resource_prefix)

        resources["cloudformation"] = list_cfn_template(resource_prefix)

        resources["cwlogs"] = list_cw_logs(resource_prefix)

        resources["datalake_settings"] = list_lake_formation_settings(resource_prefix)

        print("Writing items to delete to JSON file: delete_file.json")
        with open("delete_file.json", "w+") as delete_file:
            json.dump(resources, delete_file)

    except Exception as e:
        print(f"Error: {e}")
