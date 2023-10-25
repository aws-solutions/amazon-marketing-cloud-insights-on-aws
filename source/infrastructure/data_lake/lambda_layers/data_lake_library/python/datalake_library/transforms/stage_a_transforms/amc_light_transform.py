# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

#######################################################
# Blueprint example of a custom transformation
# where a JSON file is downloaded from RAW to /tmp
# then parsed before being re-uploaded to STAGE
#######################################################

import awswrangler as wr
import re
from aws_solutions.core.helpers import get_service_resource, get_service_client
from boto3.dynamodb.conditions import Key
import os

#######################################################
# Use S3 Interface to interact with S3 objects
# For example to download/upload them
#######################################################
from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import S3Configuration, KMSConfiguration
from datalake_library.interfaces.s3_interface import S3Interface

s3 = get_service_resource('s3')
dynamodb = get_service_resource("dynamodb")
ssm = get_service_client('ssm')

s3_interface = S3Interface()
# IMPORTANT: Stage bucket where transformed data must be uploaded


logger = init_logger()


class CustomTransform():
    def __init__(self):
        logger.info("S3 Blueprint Light Transform initiated")

    def transform_object(self, resource_prefix, bucket, key, team, dataset):

        stage_bucket = S3Configuration(resource_prefix).stage_bucket

        # show the object key received
        logger.info('SOURCE OBJECT KEY: ' + key)

        # retrieve the source file as an S3 object
        s3_object = s3.Object(bucket, key)

        # get the file size - originally we would send the file size to the email lambda to determine if it can be attached
        file_size = s3_object.content_length

        # get the file last modified date as a formatted string
        file_last_modified = s3_interface.get_last_modified(bucket, key)
        file_last_modified = file_last_modified.replace(' ', '-').replace(':', '-').split('+')[0]
        logger.info('file_last_modified: {}'.format(file_last_modified))

        # initialize our variables extracted from the key:
        key_team = key_dataset = workflow_name = schedule_frequency = file_name = file_year = file_month = file_day = file_hour = file_minute = file_second = file_millisecond = file_basename = file_extension = file_version = ''

        processed_keys = []

        # break the file key into it's name components
        key_parts = re.match("workflow=([^/]*)/schedule=([^/]*)/(.*)", key)  # USE WHEN INGESTION BUCKET OUTSIDE OF LAKE

        if key_parts is not None:
            workflow_name, schedule_frequency, file_name = key_parts.groups()  # use below 3 lines if ingesting direct from amc bucket
            key_team = team
            key_dataset = dataset

            # see if the workflow name matches the naming scheme for a AMC UI result:
            if re.match("analytics-[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}", workflow_name) is not None:
                logger.info(
                    "Workflow name {} appears to be a result from the AMC UI, skipping transformation, setting processed_keys to an empty array".format(
                        workflow_name))
                processed_keys = []
                return (processed_keys)

        #see if the key matches the file name pattern with time e.g. workflow=standard_geo_date_summary_V3/schedule=adhoc/file_last_modified=2020-02-03-12-25-38/2020-02-03T14:01:47Z-standard_geo_date_summary.csv
        file_name_with_time_search_results = re.match("(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})Z-([^\.]*)\.(.*)",file_name)

        if file_name_with_time_search_results is not None:
            file_year, file_month, file_day, file_hour, file_minute, file_second, file_millisecond, file_basename, file_extension = file_name_with_time_search_results.groups()
        else:
            # Check to see if the key matches the pattern with date only e.g. workflow=standard_geo_date_summary_V3/schedule=weekly/2020-02-04-standard_geo_date_summary_V3-ver2.csv
            file_name_date_only_search_results = re.match("(\d{4})-(\d{2})-(\d{2})-([^.]*)\.(.*)", file_name)
            if file_name_date_only_search_results is not None:
                file_year, file_month, file_day, file_basename, file_extension = file_name_date_only_search_results.groups()

        # check to see if we have a version appended to the end fo the file basename
        version_results = re.match(".*-(ver\d)", file_basename)
        if version_results is not None:
            file_version = version_results.groups()[0]

        customer_config = ssm.get_parameter(
            Name='/{}/DynamoDB/DataLake/CustomerConfig'.format(resource_prefix),
            WithDecryption=True
        ).get('Parameter').get('Value')
        config_table = dynamodb.Table(customer_config)
        response = config_table.query(
            IndexName='amc-index',
            Select='ALL_PROJECTED_ATTRIBUTES',
            KeyConditionExpression=Key('hash_key').eq(bucket)
        )
        prefix = response['Items'][0]['prefix'].lower()
        customer_hash_key = response['Items'][0]['customer_hash_key'].lower()
        logger.info('prefix: {}'.format(prefix))
        ##################################

        #read the source file into a pandas dataframe, replacing double quotes with single quotes before it is processed by pandas
        logger.info("fileExtension : " + file_extension)
        logger.info("workflowName : " + workflow_name)
        logger.info("scheduleFrequency : " + schedule_frequency)
        
        if file_extension.lower() == 'csv' and workflow_name != '' and schedule_frequency != '' :

            ### Validate small file ###
            if file_size < 1000:
                line_count = s3_object.get()['Body'].read().decode('utf-8').count('\n')
                if line_count <= 1:
                    logger.info("File empty: No data to process")
                    return processed_keys

            oob_reports = get_oob_reports(response)

            # Calculate the output path with partitioning based on the original file name
            output_path = ''

            # ### OUTPUT_PATH FOR OOB_REPORTS
            table_prefix = get_table_prefix(workflow_name, oob_reports, prefix)

            output_path = get_output_path(file_version,table_prefix,workflow_name,schedule_frequency,customer_hash_key,file_year,file_month,file_last_modified,file_basename,file_extension)

            logger.info("outputPath : " + output_path)

            output_path = os.path.splitext(output_path)[0].rsplit('/', 1)[0].split('/')
            output_path[0] = wr.catalog.sanitize_table_name(output_path[0])
            output_path = '/'.join(output_path)
            output_path = '{}/{}.{}'.format(output_path, file_basename, file_extension)

            # Uploading file to Stage bucket at appropriate path
            # IMPORTANT: Build the output s3_path without the s3://stage-bucket/
            s3_path = 'pre-stage/{}/{}/{}'.format(team,dataset, output_path)
            logger.info('S3 Path: {}'.format(s3_path))

            kms_key = KMSConfiguration(resource_prefix, "Stage").get_kms_arn

            content = s3_object.get()['Body'].read().decode("UTF8").replace('\\"', "'")

            file_meta_data = {
                'keyTeam': key_team,
                'keyDataset': key_dataset,
                'workflowName': workflow_name,
                'scheduleFrequency': schedule_frequency,
                'fileName': file_name,
                'fileYear': file_year,
                'fileMonth': file_month,
                'fileDay': file_day,
                'fileHour': file_hour,
                'fileMinute': file_minute,
                'fileSecond': file_second,
                'fileMillisecond': file_millisecond,
                'fileBasename': file_basename,
                'fileExtension': file_extension,
                'fileVersion': file_version,
                'partitionedPath': output_path.rsplit('/', 1)[0]
            }

            s3.Object(stage_bucket, s3_path).put(Body=content, ServerSideEncryption='aws:kms', SSEKMSKeyId=kms_key,
                                                 Metadata=file_meta_data
                                                 )

            # IMPORTANT S3 path(s) must be stored in a list
            processed_keys = [s3_path]

        #######################################################
        # IMPORTANT
        # This function must return a Python list
        # of transformed S3 paths. Example:
        # ['pre-stage/engineering/legislators/persons_parsed.json']
        #######################################################

        return processed_keys
    
def get_table_prefix(workflow_name, oob_reports, prefix):
    if workflow_name not in oob_reports:
        table_prefix=prefix
    else:
        table_prefix = 'amc'

    return table_prefix

def get_oob_reports(response):
    oob_reports = []
    if 'oob_reports' in response['Items'][0]:
                for wf in response['Items'][0]['oob_reports']:
                    oob_reports.append(wf)

    return oob_reports

def get_output_path(file_version, table_prefix,workflow_name,schedule_frequency,customer_hash_key,file_year,file_month,file_last_modified,file_basename,file_extension):
    if file_version != '':
        output_path = "{}_{}_{}_{}/customer_hash={}/export_year={}/export_month={}/file_last_modified={}/{}.{}".format(table_prefix,workflow_name,schedule_frequency,file_version,customer_hash_key,file_year,file_month,file_last_modified,file_basename,file_extension)
    else:
        output_path = "{}_{}_{}/customer_hash={}/export_year={}/export_month={}/file_last_modified={}/{}.{}".format(table_prefix,workflow_name,schedule_frequency,customer_hash_key,file_year,file_month,file_last_modified,file_basename,file_extension)
    
    return output_path
