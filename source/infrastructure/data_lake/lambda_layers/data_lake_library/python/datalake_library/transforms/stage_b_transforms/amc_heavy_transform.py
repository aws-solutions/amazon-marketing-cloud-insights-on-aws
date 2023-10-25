# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

#######################################################
# Blueprint example of a custom transformation
# where a number of CSV files are dowloaded from
# Stage bucket and then submitted to a Glue Job
#######################################################

#######################################################
# Import section
# common-pipLibrary repository can be leveraged
# to add external libraries as a layer
#######################################################
import json
import datetime as dt

from aws_solutions.core.helpers import get_service_client

import awswrangler as wr

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import KMSConfiguration

logger = init_logger()

# Create a client for the AWS Analytical service to use
client = get_service_client('glue')


def datetime_converter(o):
    if isinstance(o, dt.datetime):
        return o.__str__()


class CustomTransform():
    def __init__(self):
        logger.info("Glue Job Blueprint Heavy Transform initiated")

    def transform_object(self, resource_prefix, bucket, keys, team, dataset):

        ssm = get_service_client('ssm')

        silver_catalog = ssm.get_parameter(
            Name='/{}/Glue/{}/{}/StageDataCatalog'.format(resource_prefix, team, dataset),
            WithDecryption=True
        ).get('Parameter').get('Value')

        gold_catalog = 'test'
        try:
            gold_catalog = ssm.get_parameter(
                Name='/{}/Glue/{}/{}/AnalyticsDataCatalog'.format(resource_prefix, team, dataset),
                WithDecryption=True
            ).get('Parameter').get('Value')
        except Exception as e: # catch *all* exceptions
            gold_catalog = 'test'
            logger.info('No analytic db')
            logger.info(str(e))

        job_name = ssm.get_parameter(
            Name="/{}/Glue/{}/{}/SDLFHeavyTransformJobName".format(resource_prefix, team, dataset),
            WithDecryption=True
        ).get('Parameter').get('Value')


        #######################################################
        # We assume a Glue Job has already been created based on
        # customer needs. This function makes an API call to start it
        #######################################################     
        tables = []

        s3_locations_to_add = {}
         
        key_counter = 0
        logger.info('Processing Keys: {}\n-----------------'.format(keys))
        for key in keys:
            key_counter+=1 
            logger.info("key {}: {}".format(key_counter, key))
            table_path = '/'.join(key.split('/')[:4])
            logger.info("table_path:{}".format(table_path))
            table_s3_location = 's3://{}/{}/{}'.format(bucket, 'post-stage', table_path.split('/', 1)[1])
            logger.info('table_s3_location:{}'.format(table_s3_location))
            s3_locations_to_add[table_s3_location]=True
            table_partitions = '/'.join(key.split('/')[4:-1])
            logger.info('table_partitions:{}'.format(table_partitions))

            sanitized_table_name = wr.catalog.sanitize_table_name(table_path.rsplit('/')[-1])
            if sanitized_table_name not in tables:
                tables.append(
                    "{}/{}".format(sanitized_table_name, table_partitions)
                )

        unique_keys=[]
        for i in keys:
            unique_keys.append('s3://{}/{}'.format(bucket, i))

        source_locations = ','.join(unique_keys)

        # S3 Path where Glue Job outputs processed keys
        # IMPORTANT: Build the output s3_path without the s3://stage-bucket/
        processed_keys_path = 'post-stage/{}/{}'.format(team, dataset)
        source_location = 's3://{}/{}'.format(bucket, keys[0])

        output_location = 's3://{}/{}'.format(bucket, processed_keys_path)
        logger.info('trying to call job: {} \nsource_location: {} \noutput_location: {} \ntables: {}'.format(job_name,
                                                                                                             source_location,
                                                                                                             output_location,
                                                                                                             tables))

        kms_key = KMSConfiguration(resource_prefix, "Stage").get_kms_arn

        # Submitting a new Glue Job
        job_response = client.start_job_run(
            JobName=job_name,
            Arguments={
                # Specify any arguments needed based on bucket and keys (e.g. input/output S3 locations)
                '--JOB_NAME': job_name,
                '--job-bookmark-option': 'job-bookmark-disable',
                '--SOURCE_LOCATIONS': source_locations,
                '--SOURCE_LOCATION': source_location,
                '--OUTPUT_LOCATION': output_location,
                '--SILVER_CATALOG': silver_catalog,
                '--KMS_KEY': kms_key,
                '--GOLD_CATALOG': gold_catalog,
            },
            MaxCapacity=1.0
        )

        # Collecting details about Glue Job after submission (e.g. jobRunId for Glue)
        json_data = json.loads(json.dumps(
            job_response, default=datetime_converter))
        job_details = {
            "jobName": job_name,
            "jobRunId": json_data.get('JobRunId'),
            "jobStatus": 'STARTED',
            "tables": tables
        }

        #######################################################
        # IMPORTANT
        # This function must return a dictionary object with at least a reference to:
        # 1) processedKeysPath (i.e. S3 path where job outputs data without the s3://stage-bucket/ prefix)
        # 2) jobDetails (i.e. a Dictionary holding information about the job
        # e.g. jobName and jobId for Glue or clusterId and stepId for EMR
        # A jobStatus key MUST be present in jobDetails as it's used to determine the status of the job)
        # Example: {processedKeysPath' = 'post-stage/engineering/legislators',
        # 'jobDetails': {'jobName': 'legislators-glue-job', 'jobId': 'jr-2ds438nfinev34', 'jobStatus': 'STARTED'}}
        #######################################################
        response = {
            'processedKeysPath': processed_keys_path,
            'jobDetails': job_details
        }

        return response

    def check_job_status(self, processed_keys_path, job_details):
        # This function checks the status of the currently running job
        job_response = client.get_job_run(
            JobName=job_details['jobName'], RunId=job_details['jobRunId'])
        json_data = json.loads(json.dumps(
            job_response, default=datetime_converter))
        # IMPORTANT update the status of the job based on the job_response (e.g RUNNING, SUCCEEDED, FAILED)
        job_details['jobStatus'] = json_data.get('JobRun').get('JobRunState')

        #######################################################
        # IMPORTANT
        # This function must return a dictionary object with at least a reference to:
        # 1) processedKeysPath (i.e. S3 path where job outputs data without the s3://stage-bucket/ prefix)
        # 2) jobDetails (i.e. a Dictionary holding information about the job
        # e.g. jobName and jobId for Glue or clusterId and stepId for EMR
        # A jobStatus key MUST be present in jobDetails as it's used to determine the status of the job)
        # Example: {processedKeysPath' = 'post-stage/legislators',
        # 'jobDetails': {'jobName': 'legislators-glue-job', 'jobId': 'jr-2ds438nfinev34', 'jobStatus': 'RUNNING'}}
        #######################################################
        response = {
            'processedKeysPath': processed_keys_path,
            'jobDetails': job_details
        }

        return response
