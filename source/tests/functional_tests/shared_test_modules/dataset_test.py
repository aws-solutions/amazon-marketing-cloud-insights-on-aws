# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3
import time
import pytest

class DatasetTest():
    """
    A class to encapsulate functional testing for a dataset

    Attributes
    ----------
    region : str
        The AWS region to use
    aws_profile : str
        The AWS profile to use
    dataset_name : str
        The name of the dataset being tested
    raw_bucket : str
        The name of the raw data bucket
    stage_bucket : str
        The name of the stage data bucket
    mock_data_file : str
        The path to the mock data file to upload
    raw_object_key : str
        The object key for the raw data file
    prestage_object_prefix : str
        The object prefix for the pre-stage transformation
    poststage_object_prefix : str
        The object prefix for the post-stage transformation
    glue_job_wait_minutes : int
        The number of minutes to wait for the glue job to complete
    """
    def __init__(self,
        region: str, 
        aws_profile: str,
        dataset_name: str, 
        raw_bucket: str, 
        stage_bucket: str, 
        mock_data_file: str,
        raw_object_key: str,
        prestage_object_prefix: str,
        poststage_object_prefix: str,
        glue_job_wait_minutes: int = 5
    ):
        self.region = region
        self.aws_profile = aws_profile
        self.dataset_name = dataset_name
        self.raw_bucket = raw_bucket
        self.stage_bucket = stage_bucket
        self.mock_data_file = mock_data_file
        self.raw_object_key = raw_object_key
        self.prestage_object_prefix = prestage_object_prefix
        self.poststage_object_prefix = poststage_object_prefix
        self.glue_job_wait_minutes = glue_job_wait_minutes
        
        session = boto3.Session(profile_name=self.aws_profile, region_name=self.region)
        self.s3_client = session.client('s3')
        
    def run_test(self):
        """
        Runs data lake functional tests for a dataset
        """
        try:
            # upload data to raw bucket
            self.upload_prestage_data()
            
            # check that transformation A was applied
            self.stage_testing(self.prestage_object_prefix)
            
            print(f"\nWaiting {self.glue_job_wait_minutes} minutes for heavy transform glue job to run...")
            time.sleep(self.glue_job_wait_minutes * 60)
            
            # check that transformation B was applied
            self.stage_testing(self.poststage_object_prefix)
            
        except Exception as _:
            pytest.fail("Error encountered during testing, stopping the test.")
        
    def upload_prestage_data(self):
        """
        Uploads mock data to the designated raw bucket for testing
        """
        print(f'\n\nUploading test data to bucket: {self.raw_bucket}\nObject: {self.raw_object_key}')
        try:
            with open(self.mock_data_file, "rb") as f:
                self.s3_client.upload_fileobj(f, self.raw_bucket, self.raw_object_key)
        except Exception as e:
            print(e)

    def stage_testing(self, object_prefix: str):
        """
        Checks for the presence of a particular file in the stage bucket

        Parameters
        ----------
        object_prefix : str
            The object prefix to check the stage bucket for
        """
        status = "NO_FILE"
        tries = 0
        while ((status == "NO_FILE") and (tries <= 2)):
            tries += 1
            try:
                print(f'\nattempt # {tries} of 3')
                print('waiting 2 minutes for data to post...')
                time.sleep(120)
                print(f'checking bucket {self.stage_bucket} for file')
                response = self.s3_client.list_objects(Bucket=self.stage_bucket, Prefix=object_prefix)
                if response['Contents']:
                    status = "FILE_FOUND"
                    print(f'status: {status}')
            except KeyError:
                print(f'status: {status}')
            except Exception as e:
                print(f'ERROR: {e}')
                break

        if status == "NO_FILE":
            pytest.fail(f'File prefix {object_prefix} not found in bucket {self.stage_bucket}')
    