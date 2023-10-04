# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import boto3
import json
import sys

profile_name = str(sys.argv[1])

session = boto3.session.Session(profile_name=profile_name)

s3_client = session.client('s3')
s3_resource = session.resource('s3')
dynamodb_client = session.client('dynamodb')
kms_client = session.client('kms')
sqs_client = session.client("sqs")
lambda_client = session.client("lambda")
events_client = session.client('events')
cfn_client = session.client('cloudformation')
cw_client = session.client('logs')
lake_formation_client = boto3.client('lakeformation')


def add_bucket_lifecycle(bucket_name):
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            'Rules': [
                {
                    'Status': 'Enabled',
                    'Expiration': {
                        'Days': 1
                    },
                    'Filter': {
                        'Prefix': 'AWSLogs'
                    }
                }
            ]
        }
    )


def empty_bucket(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    versions = s3_client.list_object_versions(Bucket=bucket_name)  # list all versions in this bucket
    if 'Contents' in response:
        if response['KeyCount'] > 100:
            add_bucket_lifecycle(bucket_name=bucket_name)
            print(f'Deletion lifecycle config added for bucket: {bucket_name}')
        else:
            for item in response['Contents']:
                print('Deleting file', item['Key'])
                s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
    if 'Versions' in versions and len(versions['Versions']) > 0:
        s3_bucket = s3_resource.Bucket(bucket_name)
        s3_bucket.object_versions.delete()


def delete_bucket(bucket_name):
    bucket = s3_resource.Bucket(bucket_name)
    bucket.object_versions.delete()
    bucket.objects.all().delete()
    s3_client.delete_bucket(Bucket=bucket_name)


def delete_table(table_name):
    dynamodb_client.delete_table(TableName=table_name)


def schedule_key_deletion(key_id):
    kms_client.schedule_key_deletion(
        KeyId=key_id,
        PendingWindowInDays=7
    )


def delete_queue(queue_url):
    sqs_client.delete_queue(
        QueueUrl=queue_url
    )


def delete_lambda_layer(layer):
    lambda_client.delete_layer_version(
        LayerName=layer["layerName"],
        VersionNumber=layer["version"]
    )


def delete_rule(rule_name):
    targets = []
    response = events_client.list_targets_by_rule(
        Rule=rule_name
    )
    for target in response["Targets"]:
        targets.append(target["Id"])

    events_client.remove_targets(
        Rule=rule_name,
        Ids=targets
    )

    events_client.delete_rule(
        Name=rule_name
    )


def delete_cfn_stack(stack_name):
    cfn_client.delete_stack(
        StackName=stack_name
    )


def delete_log_group(log_group):
    cw_client.delete_log_group(
        logGroupName=log_group
    )


if __name__ == "__main__":
    try:
        with open('delete_file.json') as json_data:
            items = json.load(json_data)

            if len(items["s3"]) > 0:
                for s3_bucket in items["s3"]:
                    print(f"Emptying content from: {s3_bucket}")
                    empty_bucket(s3_bucket)
                    print(f"Bucket emptied: {s3_bucket}")

                for s3_bucket in items["s3"]:
                    print(f"Deleting bucket: {s3_bucket}")
                    delete_bucket(s3_bucket)
                    print(f"Bucket deleted: {s3_bucket}")

            if len(items["dynamodb"]) > 0:
                for ddb_table in items["dynamodb"]:
                    print(f"Deleting table: {ddb_table}")
                    delete_table(ddb_table)
                    print(f"Table deleted: {ddb_table}")

            if len(items["sqs"]) > 0:
                for queue_url in items["sqs"]:
                    print(f"Deleting SQS queue: {queue_url}")
                    delete_queue(queue_url)
                    print(f"Queue deleted: {queue_url}")

            if len(items["lambda_layer"]) > 0:
                for layer in items["lambda_layer"]:
                    print(layer)
                    print(f"Deleting Lambda layer: {layer['layerName']} Version {layer['version']}")
                    delete_lambda_layer(layer)
                    print(f"Lambda layer deleted: {layer}")

            if len(items["event_bridge"]) > 0:
                for rule in items["event_bridge"]:
                    print(f"Deleting Eventbridge rule: {rule}")
                    delete_rule(rule)
                    print(f"Eventbridge rule deleted: {rule}")

            if len(items["cloudformation"]) > 0:
                for stack in items["cloudformation"]:
                    print(f"Deleting Cloudformation stack: {stack}")
                    delete_cfn_stack(stack)
                    print(f"Cloudformation stack deleted: {stack}")

            if len(items["cwlogs"]) > 0:
                for log_group in items["cwlogs"]:
                    print(f"Deleting log group: {log_group}")
                    delete_log_group(log_group)
                    print(f"Log group deleted: {log_group}")

            if len(items["kms"]) > 0:
                for key_id in items["kms"]:
                    print(f"Scheduling KMS key delete: {key_id}")
                    schedule_key_deletion(key_id)
                    print(f"Key deletion scheduled: {key_id}")

            if len(items["datalake_settings"]) > 0:
                lake_formation_client.put_data_lake_settings(DataLakeSettings=items["datalake_settings"])

            json_data.close()

        print("Empty items in JSON file: delete_file.json")
        with open("delete_file.json", "w+") as delete_file:
            json.dump({}, delete_file)

    except Exception as e:
        print(f"Error: {e}")
