# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3
import json
import os
from botocore.exceptions import ClientError

class V2Interface():
   def __init__(self):
      self.dynamodb_client = boto3.client('dynamodb') 
      self.s3_resource = boto3.resource('s3')
      self.lambda_client = boto3.client('lambda')

   def get_instance_buckets(self, table_name):
      print(f'getting amc instance bucket names from table: {table_name}')

      response = self.dynamodb_client.scan(TableName=table_name)
      items = response['Items']
      while 'LastEvaluatedKey' in response:
         response = self.dynamodb.scan(TableName=table_name, ExclusiveStartKey=response['LastEvaluatedKey'])
         items.extend(response['Items'])

      bucket_list = []
      for record in items:
         print(f"bucket found: {record['BucketName']['S']}")
         bucket_list.append(record['BucketName']['S'])
      
      return bucket_list
   
   @staticmethod
   def paginate_results(iterator):
      keys = []
      for response in iterator:
         if 'Contents' in response:
                  for obj in response['Contents']:
                     # skip metadata files
                     if obj['Key'].endswith(".json"):
                        continue
                     keys.append(obj['Key'])
      return keys
   
   def get_object_keys(self, bucket_name):
      if 'BUCKET_PROFILE' in os.environ:
         session = boto3.Session(profile_name=os.environ['BUCKET_PROFILE'])
         cross_s3 = session.client('s3')

      s3 = boto3.client('s3')
      keys = []

      try:
         paginator = s3.get_paginator('list_objects_v2')
         response_iterator = paginator.paginate(Bucket=bucket_name)
         keys = self.paginate_results(iterator=response_iterator)
      # if access denied, bucket must be in cross account
      except ClientError:
         try:
            paginator = cross_s3.get_paginator('list_objects_v2')
            response_iterator = paginator.paginate(Bucket=bucket_name)
            keys = self.paginate_results(iterator=response_iterator)
         except UnboundLocalError:
            raise UnboundLocalError("Cross account bucket detected. Run script again using --bucket-profile argument.")

      return keys

   def send_trigger_event(self, lambda_name, event):
      object_key = event['detail']['requestParameters']['key']
      print(f"sending event for key: {object_key}")

      response = self.lambda_client.invoke(
         FunctionName=lambda_name,
         InvocationType='RequestResponse',
         LogType='Tail',
         Payload=json.dumps(event).encode('UTF-8')
      )
      
      return response['ResponseMetadata']['HTTPStatusCode']
      