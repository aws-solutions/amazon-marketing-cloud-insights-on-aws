# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3
import json
import time
import subprocess

#########################
##    SETUP  TESTS    ###
#########################

STACK_NAME = os.environ['STACK']
REGION = os.environ['REGION']
PROFILE = os.environ['DEFAULT_PROFILE']
CUSTOMER_ID = os.environ['CUSTOMER_ID']
AMC_INSTANCE_ID = os.environ['AMC_INSTANCE_ID']
AMAZON_ADS_ADVERTISER_ID = os.environ['AMAZON_ADS_ADVERTISER_ID']
AMAZON_ADS_MARKETPLACE_ID = os.environ['AMAZON_ADS_MARKETPLACE_ID']


def get_cross_account_profile():
    check = os.environ['TEST_CROSS_ACCOUNT']
    if check == "Yes":
        return os.environ['BUCKET_PROFILE']


def get_dataset_config():
    check = os.environ['TEST_AMC_DATASET']
    if check == "Yes":
        return True
    else:
        return False


def get_aws_account_from_profile(profile):
    session = boto3.Session(profile_name=profile)
    sts_client = session.client('sts')
    response = sts_client.get_caller_identity()
    aws_account = response['Account']
    return aws_account


def get_cross_region():
    check = os.environ["TEST_CROSS_REGION"]
    if check == "YES":
        if REGION == 'us-east-1':
            return 'us-east-2'
        else:
            return 'us-east-1'
    else:
        return None


account_id = get_aws_account_from_profile(profile=PROFILE)
cross_region = get_cross_region()
test_amc_dataset = get_dataset_config()


class AMCTest:
    def __init__(
            self,
            deployment_region,
            orange_account,
            instance_id,
            amazon_ads_advertiser_id,
            amazon_ads_marketplace_id,
            customer_id,
            stack_name,
            profile
    ):
        self.profile = profile
        # boto3 testing
        self.session = boto3.Session(profile_name=profile)
        self.cfn_client = self.session.client('cloudformation', region_name=REGION)
        self.sts_client = self.session.client('sts', region_name=REGION)
        self.s3_client = self.session.client('s3', region_name=REGION)
        self.s3_resource = self.session.resource('s3', region_name=REGION)
        # microservices
        self.customer_id = customer_id
        self.stack_name = stack_name
        self.deployment_region = deployment_region
        self.orange_account = orange_account
        self.instance_id = instance_id
        self.amazon_ads_advertiser_id = amazon_ads_advertiser_id
        self.amazon_ads_marketplace_id = amazon_ads_marketplace_id
        self.customer_stack_name = f"{self.stack_name}-tps-instance-{self.customer_id}"
        self.tps_sm_lambda = f"{self.stack_name}-tps-InvokeTPSInitializeSM"
        self.amc_bucket = f"amc-{self.customer_id}"
        self.request = {
            "customer_details": {
                "customer_id": self.customer_id,
                "customer_name": "TestCustomer",
                "bucket_region": self.deployment_region,
                "bucket_exists": "false",
                "amc": {
                    "aws_orange_account_id": self.orange_account,
                    "bucket_name": self.amc_bucket,
                    "instance_id": self.instance_id,
                    "amazon_ads_advertiser_id": self.amazon_ads_advertiser_id,
                    "amazon_ads_marketplace_id": self.amazon_ads_marketplace_id,
                }
            }
        }
        # data lake
        self.data_file = "mock_data.csv"
        self.object_key = f"workflow=test-{self.customer_id}/schedule=adhoc/2023-05-02T21:42:25.904Z-test-{self.customer_id}.csv"
        self.pre_stage_prefix = f'pre-stage/adtech/amc/{self.customer_id}_test_{self.customer_id}_adhoc/customer_hash={self.customer_id}/'
        self.post_stage_prefix = f'post-stage/adtech/amc/{self.customer_id}_test_{self.customer_id}_adhoc/customer_hash={self.customer_id}/'
        # cleanup
        self.stage_suffix = f"adtech/amc/{self.customer_id}_test_{self.customer_id}_adhoc/customer_hash={self.customer_id}/"
        self.table_keys = {
            'sdlf_customer_config': {
                'customer_hash_key': self.customer_id,
                'hash_key': self.amc_bucket
            },
            'tps_customer_config': {
                'customerId': self.customer_id,
                'customerName': self.customer_id
            },
            'wfm_customer_config': {
                'customerId': self.customer_id
            }
        }

    #########################
    #####    TESTING    #####
    #########################
    def onboard_customer(self):
        lambda_client = boto3.Session(profile_name=PROFILE, region_name=REGION).client('lambda')
        response = lambda_client.invoke(
            FunctionName=self.tps_sm_lambda,
            InvocationType='RequestResponse',
            LogType='Tail',
            Payload=json.dumps(self.request).encode('UTF-8'),
        )
        response_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        print(f"lambda response received for {self.customer_id}: {response_code}")
        return response_code

    @staticmethod
    def get_stack_status(stack_name, cfn_client):
        response = cfn_client.describe_stacks(StackName=stack_name)
        stack_status = response['Stacks'][0]['StackStatus']
        print(f"stack status: {stack_status}")
        return stack_status

    def check_stack(self, stack_name, region=REGION):
        cfn_client = boto3.Session(profile_name=PROFILE).client('cloudformation', region_name=region)
        print(f"\nchecking test stack: {stack_name}")
        stack_status = self.get_stack_status(stack_name=stack_name, cfn_client=cfn_client)
        while stack_status not in ['CREATE_COMPLETE', 'ROLLBACK_COMPLETE', 'ROLLBACK_IN_PROGRESS', 'CREATE_FAILED']:
            print("waiting 60 seconds to check stack status again...")
            time.sleep(60)
            stack_status = self.get_stack_status(stack_name=stack_name, cfn_client=cfn_client)
        return stack_status

    def upload_data(self):
        print(f'\nuploading test data to bucket: {self.amc_bucket}\nobject key: {self.object_key}')
        try:
            with open(self.data_file, "rb") as f:
                self.s3_client.upload_fileobj(f, self.amc_bucket, self.object_key)
        except Exception as e:
            print(e)

    def check_stage(self, prefix):
        s3_client = boto3.Session(profile_name=PROFILE).client('s3')
        status = "ERROR"
        tries = 0
        while ((status == "ERROR") and (tries <= 3)):
            tries += 1
            try:
                print(f'\nattempt # {tries} of 3')
                print('waiting 60 seconds for data to post...')
                time.sleep(60)
                print(f'checking bucket {os.environ["STAGE_BUCKET"]} for file {prefix}')
                response = s3_client.list_objects(Bucket=os.environ["STAGE_BUCKET"], Prefix=prefix)
                if response['Contents']:
                    status = "SUCCESS"
            except KeyError:
                print('file not found')
                status = "ERROR"
            except Exception as e:
                print(f'error: {e}')
                status = "ERROR"

        print(f"stage status: {status}")
        return status

    #########################
    #####    CLEANING   #####
    #########################
    def delete_stack(self):
        # clean up cloudformation stacks
        try:
            print(f'\ndeleting stack {self.customer_stack_name}')
            cfn_client = boto3.Session(profile_name=PROFILE, region_name=REGION).client('cloudformation')
            # delete main region stack
            cfn_client.delete_stack(StackName=self.customer_stack_name)
            if self.deployment_region != REGION:
                cross_region_client = boto3.Session(profile_name=PROFILE, region_name=cross_region).client(
                    'cloudformation')
                # delete cross region stack
                cross_region_client.delete_stack(StackName=self.customer_stack_name + "-crossregion")
            if self.profile != PROFILE:
                cross_account_client = boto3.Session(profile_name=self.profile, region_name=REGION).client(
                    'cloudformation')
                # delete cross account stack
                cross_account_client.delete_stack(StackName=self.customer_stack_name)
            else:
                if self.deployment_region != REGION:
                    access_logs_stack = f"{STACK_NAME}-crossregion-tps-{cross_region}-access-logs"
                    print(f"deleting stack: {access_logs_stack}")
                    # delete access logs stack cross region
                    cross_region_client.delete_stack(StackName=access_logs_stack)
                else:
                    access_logs_stack = f"{STACK_NAME}-tps-{self.deployment_region}-access-logs"
                    print(f"deleting stack: {access_logs_stack}")
                    # delete access logs main region stack
                    cfn_client.delete_stack(StackName=access_logs_stack)

        except Exception as e:
            print(f"error cleaning up TPS tests: {e}")

    def clean_table(self, table_name: str, key: dict):
        print(f'\ndeleting record from {table_name} customer config table')
        dynamodb_resource = boto3.Session(profile_name=PROFILE, region_name=REGION).resource('dynamodb')
        table = dynamodb_resource.Table(table_name)
        try:
            response = table.delete_item(
                Key=key
            )
        except Exception as e:
            response = e
        print(f'dynamodb response: {response["ResponseMetadata"]["HTTPStatusCode"]}')

    def get_object_versions(self, bucket_name, profile):
        s3_client = boto3.Session(profile_name=profile).client('s3')
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        versions = s3_client.list_object_versions(Bucket=bucket_name)

        return response, versions

    def delete_objects(self, item, bucket_name, response, profile):
        s3_client = boto3.Session(profile_name=profile).client('s3')
        print('deleting file', item['Key'])
        s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
        while response['KeyCount'] == 1000:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                StartAfter=response['Contents'][0]['Key'],
            )
            for item in response['Contents']:
                print('deleting file', item['Key'])
                s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
        print("test file deleted")

    def delete_version(self, bucket_name, item_list, profile):
        s3_resource = boto3.Session(profile_name=profile).resource('s3')
        s3_bucket = s3_resource.Bucket(bucket_name)
        for i in item_list:
            s3_bucket.object_versions.filter(Prefix=i).delete()

    def delete_versioned_objects(self, bucket_name, response, versions, profile):
        item_list = []
        if 'Contents' in response:
            for item in response['Contents']:
                if (item['Key'].startswith(f"post-stage/{self.stage_suffix}") or item['Key'].startswith(
                        f"pre-stage/{self.stage_suffix}")):
                    try:
                        self.delete_objects(item=item, bucket_name=bucket_name, response=response, profile=profile)
                        item_list.append(item['Key'])
                    except Exception as e:
                        print(f'failed to delete file: {e}')

        if 'Versions' in versions and len(versions['Versions']) > 0:
            print('deleting object versions')
            try:
                self.delete_version(bucket_name=bucket_name, item_list=item_list, profile=profile)
                print('object versions deleted')
            except Exception as e:
                print(f"error deleting versions: {e}")

    def delete_bucket(self, bucket_name, profile):
        print(f'\nemptying & deleting amc bucket: {bucket_name}')
        s3_client = boto3.Session(profile_name=profile).client('s3')
        s3_resource = boto3.Session(profile_name=profile).resource('s3')
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            versions = s3_client.list_object_versions(Bucket=bucket_name)
            if 'Contents' in response:
                for item in response['Contents']:
                    print('Deleting file', item['Key'])
                    s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
            if 'Versions' in versions and len(versions['Versions']) > 0:
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.object_versions.delete()
            print("bucket emptied")
        except Exception as e:
            print(f'error deleting test file:\n{e}')
        try:
            bucket = s3_resource.Bucket(bucket_name)
            bucket.object_versions.delete()
            bucket.objects.all().delete()
            s3_client.delete_bucket(Bucket=bucket_name)
            print(f"{bucket_name} deleted successfully")
        except Exception as e:
            print(f'error deleting bucket:\n{e}')

    def clean_bucket(self, bucket_name, profile):
        print(f'\ndeleting test data from stage bucket: {bucket_name}')
        try:
            response, versions = self.get_object_versions(bucket_name=bucket_name, profile=profile)
        except Exception as e:
            print(f"error getting objects from bucket: {e}")
            return
        self.delete_versioned_objects(bucket_name=bucket_name, response=response, versions=versions, profile=profile)
        print("test data deleted")

    def delete_bucket_trail(self, bucket_name, trail):
        print(f'\ndeleting {bucket_name} from cloudtrail: {trail}')
        try:
            cloudtrail = boto3.Session(profile_name=PROFILE, region_name=REGION).client('cloudtrail')
            response = cloudtrail.get_event_selectors(TrailName=trail)
            event_selectors = response['AdvancedEventSelectors']
            for i in event_selectors:
                if i['Name'] == "S3EventSelector":
                    for y in i['FieldSelectors']:
                        if y['Field'] == 'resources.ARN':
                            y['StartsWith'].remove(f"arn:aws:s3:::{bucket_name}/")
            cloudtrail.put_event_selectors(
                TrailName=trail,
                AdvancedEventSelectors=event_selectors
            )
        except Exception as e:
            print(f"error removing bucket from cloudtrail: {e}")
            return
        print("bucket removed from cloudtrail")


def test_amc():
    #########################
    ##### MICROSERVICES #####
    #########################
    print('\n\n*starting tests for TPS service*')

    default_test = AMCTest(
        deployment_region=REGION,
        orange_account=account_id,
        instance_id=AMC_INSTANCE_ID,
        amazon_ads_advertiser_id=AMAZON_ADS_ADVERTISER_ID,
        amazon_ads_marketplace_id=AMAZON_ADS_MARKETPLACE_ID,
        customer_id=CUSTOMER_ID + "default",
        stack_name=STACK_NAME,
        profile=PROFILE
    )

    print("\ninvoking tps onboarding lambda")
    print("\nonboarding default customer")
    response_code = default_test.onboard_customer()
    assert response_code in range(200, 204)

    if cross_region:
        print("\nwaiting 10 seconds to onboard cross-region customer")
        time.sleep(10)
        
        cross_region_test = AMCTest(
            deployment_region=cross_region,
            orange_account=account_id,
            instance_id=AMC_INSTANCE_ID,
            amazon_ads_advertiser_id=AMAZON_ADS_ADVERTISER_ID,
            amazon_ads_marketplace_id=AMAZON_ADS_MARKETPLACE_ID,
            customer_id=CUSTOMER_ID + "crossregion",
            stack_name=STACK_NAME,
            profile=PROFILE
        )

        response_code = cross_region_test.onboard_customer()
        assert response_code in range(200, 204)

    print("\nwaiting 5 seconds for stack creation to begin")
    time.sleep(5)

    '''
    Confirm the stack successfully deployed
    '''
    stack_status = default_test.check_stack(default_test.customer_stack_name)
    assert stack_status == "CREATE_COMPLETE"

    if cross_region:
        stack_status = cross_region_test.check_stack(cross_region_test.customer_stack_name)
        assert stack_status == "CREATE_COMPLETE"
        stack_status = cross_region_test.check_stack(cross_region_test.customer_stack_name + "-crossregion",
                                                    region=cross_region)
        assert stack_status == "CREATE_COMPLETE"

    print("\nwaiting 90 seconds for TPS step functions to finish")
    time.sleep(90)

    #########################
    #####  AMC DATASET  #####
    #########################
    if test_amc_dataset:
        print('\n*starting amc-dataset tests*')
        default_test.upload_data()
        if cross_region:
            cross_region_test.upload_data()
        print("\nwaiting 5 minutes for glue job to run")
        time.sleep(300)

        print(f'\ntesting {default_test.customer_id} in data lake')
        post_stage_status = default_test.check_stage(prefix=default_test.post_stage_prefix)
        assert post_stage_status == "SUCCESS"

        if cross_region:
            print(f'\ntesting {cross_region_test.customer_id} in data lake')
            post_stage_status = cross_region_test.check_stage(prefix=cross_region_test.post_stage_prefix)
            assert post_stage_status == "SUCCESS"

    #########################
    #### CROSS ACCOUNT  ####
    #########################
    cross_account_profile = get_cross_account_profile()
    if cross_account_profile:
        cross_account_session = boto3.Session(profile_name=cross_account_profile, region_name=REGION)
        cross_account_id = get_aws_account_from_profile(profile=cross_account_profile)

        print('\n\n*starting tests for cross account*')

        cross_account_test = AMCTest(
            deployment_region=REGION,
            orange_account=account_id,
            instance_id=AMC_INSTANCE_ID,
            amazon_ads_advertiser_id=AMAZON_ADS_ADVERTISER_ID,
            amazon_ads_marketplace_id=AMAZON_ADS_MARKETPLACE_ID,
            customer_id=CUSTOMER_ID + "crossaccount",
            stack_name=STACK_NAME,
            profile=cross_account_profile
        )
        cross_account_test.request = {
            "customer_details": {
                "customer_id": cross_account_test.customer_id,
                "customer_name": "TestCustomer",
                "bucket_account": cross_account_id,
                "amc": {
                    "aws_orange_account_id": cross_account_test.orange_account,
                    "bucket_name": cross_account_test.amc_bucket,
                    "instance_id": cross_account_test.instance_id,
                    "amazon_ads_advertiser_id": cross_account_test.amazon_ads_advertiser_id,
                    "amazon_ads_marketplace_id": cross_account_test.amazon_ads_marketplace_id,
                }
            }
        }
        print("\ninvoking tps onboarding lambda")
        print("\nonboarding cross account customer")
        response_code = cross_account_test.onboard_customer()
        assert response_code in range(200, 204)

        print("\nwaiting 10 seconds for stack creation to begin")
        time.sleep(10)

        stack_status = cross_account_test.check_stack(cross_account_test.customer_stack_name)
        assert stack_status == "CREATE_COMPLETE"

        print("\nwaiting 90 seconds for TPS step functions to finish")
        time.sleep(90)

        print(f"\ncreating resources in cross account: {cross_account_id}")
        cross_account_s3 = cross_account_session.client('s3')

        # create bucket
        if REGION != "us-east-1":
            cross_account_s3.create_bucket(Bucket=cross_account_test.amc_bucket,
                                       CreateBucketConfiguration={'LocationConstraint': REGION})
        else:
            cross_account_s3.create_bucket(Bucket=cross_account_test.amc_bucket)
            
        time.sleep(10)

        # enable event bridge on the bucket so that notifications are sent
        command = [
            "aws",
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            cross_account_test.amc_bucket,
            "--notification-configuration",
            '{"EventBridgeConfiguration": {}}',
            "--profile",
            cross_account_profile,
            "--region",
            REGION
        ]
        subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # get template
        response = boto3.Session(profile_name=PROFILE, region_name=REGION).client('cloudformation').describe_stacks(
            StackName=cross_account_test.customer_stack_name)
        stack = response['Stacks'][0]
        outputs = stack.get('Outputs', [])
        template = outputs[0]['OutputValue']

        # deploy template
        cross_account_cfn = cross_account_session.client('cloudformation')
        cross_account_cfn.create_stack(
            StackName=cross_account_test.customer_stack_name,
            TemplateURL=template,
            Capabilities=[
                'CAPABILITY_IAM'
            ]
        )
        print("\nwaiting 5 minutes for template to deploy")
        time.sleep(300)

        response = cross_account_cfn.describe_stacks(StackName=cross_account_test.customer_stack_name)
        check = len(response['Stacks']) > 0
        assert check

        if test_amc_dataset and check:
            print('\n*uploading data to cross account bucket*')
            cross_account_test.upload_data()
            print("\nwaiting 5 minutes for glue job to run")
            time.sleep(300)

            post_stage_status = cross_account_test.check_stage(prefix=cross_account_test.post_stage_prefix)
            assert post_stage_status == "SUCCESS"
