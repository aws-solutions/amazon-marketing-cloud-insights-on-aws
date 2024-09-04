# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3

class DatasetClean():
    """
    A class to encapsulate cleanup for DatasetTest

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
        raw_bucket: str, 
        stage_bucket: str, 
        raw_object_key: str,
        prestage_object_prefix: str,
        poststage_object_prefix: str,
    ):
        self.region = region
        self.aws_profile = aws_profile
        self.raw_bucket = raw_bucket
        self.stage_bucket = stage_bucket
        self.raw_object_key = raw_object_key
        self.prestage_object_prefix = prestage_object_prefix
        self.postage_object_prefix = poststage_object_prefix
        
        session = boto3.Session(profile_name=self.aws_profile, region_name=self.region)
        self.s3_resource = session.resource('s3')
        self.s3_client = session.client('s3')
        
    def delete_version(self, bucket_name, item_list):
        """
        Deletes specific versions of objects from the given S3 bucket.

        Parameters
        ----------
        bucket_name : str
            The name of the S3 bucket from which to delete object versions.
        item_list : list of str
            A list of object keys to delete from the bucket.
        """
        s3_bucket = self.s3_resource.Bucket(bucket_name)
        for i in item_list:
            s3_bucket.object_versions.filter(Prefix=i).delete()
    
    def delete_versioned_objects(self, bucket_name, response, versions):
        """
        Deletes versioned objects from the given S3 bucket based on the response and versions.

        Parameters
        ----------
        bucket_name : str
            The name of the S3 bucket from which to delete versioned objects.
        response : dict
            The response dictionary from the list_objects_v2 call.
        versions : dict
            The versions dictionary from the list_object_versions call.
        """
        item_list = []
        if 'Contents' in response:
            for item in response['Contents']:
                if (
                    item['Key'].startswith(self.prestage_object_prefix) or 
                    item['Key'].startswith(self.postage_object_prefix) or 
                    item['Key'].startswith(self.raw_object_key)
                ):
                    try:
                        self.delete_objects(item=item, bucket_name=bucket_name, response=response)
                        item_list.append(item['Key'])
                    except Exception as e:
                        print(f'Failed to delete file: {e}')
        if 'Versions' in versions and len(versions['Versions']) > 0:
            try:
                self.delete_version(bucket_name=bucket_name, item_list=item_list)
            except Exception as e:
                print(f"Error deleting versions: {e}")
    
    def delete_objects(self, item, bucket_name, response):
        """
        Deletes individual objects from the S3 bucket based on the given item and response.

        Parameters
        ----------
        item : dict
            The dictionary containing the S3 object details to be deleted.
        bucket_name : str
            The name of the S3 bucket from which to delete objects.
        response : dict
            The response dictionary from the list_objects_v2 call.
        """
        self.s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
        while response['KeyCount'] == 1000:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                StartAfter=response['Contents'][0]['Key'],
            )
            for item in response['Contents']:
                self.s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
                
    def get_object_versions(self, bucket_name):
        """
        Retrieves the list of objects and their versions from the given S3 bucket.

        Parameters
        ----------
        bucket_name : str
            The name of the S3 bucket from which to retrieve object versions.

        Returns
        -------
        tuple
            A tuple containing two dictionaries: the response from list_objects_v2 and the response from list_object_versions.
        """
        response = self.s3_client.list_objects_v2(Bucket=bucket_name)
        versions = self.s3_client.list_object_versions(Bucket=bucket_name)

        return response, versions
    
    def clean_bucket(self, bucket_name):
        """
        Cleans the specified S3 bucket by deleting versioned objects.

        Parameters
        ----------
        bucket_name : str
            The name of the S3 bucket to be cleaned.
        """
        try:
            response, versions = self.get_object_versions(bucket_name=bucket_name)
        except Exception as e:
            print(f"Error getting objects from bucket: {e}")
            return
        self.delete_versioned_objects(bucket_name=bucket_name, response=response, versions=versions)
        
    def clean_s3_tests(self):
        """
        Cleans test data from both the raw and stage S3 buckets.
        """
        print(f"Deleting test data from {self.raw_bucket}")
        self.clean_bucket(bucket_name=self.raw_bucket)
        
        print(f"Deleting test data from {self.stage_bucket}")
        self.clean_bucket(bucket_name=self.stage_bucket)
        