# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3
import json
import os
from boto3.dynamodb.types import TypeDeserializer

from modules.data_config import DataConfig


class V1Interface():
    def __init__(self):
        self.dynamodb_client = boto3.client('dynamodb')

    @staticmethod
    def deserialize_dynamodb_item(item: dict) -> dict:
        return {k: TypeDeserializer().deserialize(value=v) for k, v in item.items()} 
    
    @staticmethod
    def convert_to_cron(custom: str) -> str:
        # we slice only the schedule expression from the string custom(* * *)
        expression = custom[7:-1]
        values = expression.split()

        minute = "0"
        hour = values[2]
        day_of_month = "*"
        month = "*"
        day_of_week = "*"
        year = "*"

        if values[0] == "H":
            day_of_month = "?"
        if values[0] == "W":
            day_of_week = values[1]
            day_of_month = "?"
        if values[0] == "D":
            day_of_month = "?"
        if values[0] == "M":
            day_of_month = values[1]
            day_of_week = "?"
        
        cron = ("cron(" + minute)
        for i in [hour, day_of_month, month, day_of_week, year]:
            cron += (' ' + i)
        cron += ")"
        return cron
    
    def get_scheduled_workflow(self, workflow_id, filepath):
        try:
            with open(filepath, 'r') as f:
                workflows = json.load(f)
            for workflow in workflows:
                if workflow["workflowId"] == workflow_id:
                    return workflow
        except Exception:
            return None
        
    
    def save_records(self, table_name, filepath):
        print(f'getting records from table: {table_name}')

        response = self.dynamodb_client.scan(TableName=table_name)
        records = response['Items']
        while 'LastEvaluatedKey' in response:
            response = self.dynamodb.scan(TableName=table_name, ExclusiveStartKey=response['LastEvaluatedKey'])
            records.extend(response['Items'])

        deserialized_records = []
        for record in records:
            deserialized_records.append(self.deserialize_dynamodb_item(item=record))

        print(f'saving records to filepath: {filepath}')

        with open(filepath, 'w') as f:
            json.dump(deserialized_records, f)

    def copy_records(self, read_fp, write_fp, table_name):
        print(f'reading records from filepath: {read_fp}')

        try:
            with open(read_fp, 'r') as f:
                data = json.load(f)
        except Exception:
            print(f"skipping data upload, {read_fp} filepath does not exists")
            return
        
        print(f'writing records to table: {table_name}')

        for record in data:
            try:
                self.dynamodb_client.put_item(
                    TableName=table_name,
                    Item=record["workflowRequest"]
                )
                data.remove(record)
            except Exception as e:
                print(f"\nERROR: {e}\n")
                continue
        
        try:
            os.remove(read_fp)
        except Exception:
            pass

        if len(data) > 0:
            print(f"failed record(s) found. Check {write_fp} filepath")
            with open(write_fp, "w") as f:
                json.dump(data, f)

    def format_customer_record(self, record):
        invalid_flag = False

        try:
            formatted_record = {
                "customer_details" : {
                    "customer_id": record['customerId'],
                    "customer_name": record['customerName'],
                    "bucket_exists": 'true',
                    "amc": {
                        "endpoint_url": record['AMC']['amcApiEndpoint'],
                        "aws_orange_account_id": record['AMC']['amcOrangeAwsAccount'],
                        "bucket_name": record['AMC']['amcS3BucketName'],
                    }
                }
            }

        except Exception:
            return [None, None, True]

        try:
            formatted_record["customer_details"]["bucket_region"] = record['bucketRegion']
        except Exception:
            formatted_record["customer_details"]["bucket_region"] = "<VALUE MISSING>"
            invalid_flag = True
        try:
            formatted_record["customer_details"]["amc"]["aws_red_account_id"] = record['connectedAwsAccountId']
        except Exception:
            formatted_record["customer_details"]["amc"]["aws_red_account_id"] = "<VALUE MISSING>"
            invalid_flag = True

        return [formatted_record, invalid_flag, False]

    
    def format_workflow_record(self, record):
        invalid_flag = False

        try:
            sql_query = " ".join(record['sqlQuery'].split())
            formatted_record =  {
                "customerId": record["customerId"],
                "workflowRequest": {
                    "customerId": record["customerId"],
                    "requestType": "createWorkflow",
                    "workflowDefinition": {
                        "sqlQuery": sql_query,
                        "workflowId": record['workflowId']
                    }
                }
            }
            if "filteredMetricsDiscriminatorColumn" in record.keys(): 
                formatted_record['workflowRequest']['workflowDefinition']['filteredMetricsDiscriminatorColumn'] = record['filteredMetricsDiscriminatorColumn']
        except Exception:
            return [None, None, True]

        return [formatted_record, invalid_flag, False]

    
    def format_schedule_record(self, record):
        invalid_flag = False

        if record["State"] == "DISABLED":
            print(f"skipping disbaled schedule {record}")
            return [None, None, True]

        try:
            params = record["Input"]["payload"]
            formatted_record = {
                "execution_request": {
                    "customerId": record["customerId"],
                    "requestType": "createExecution",
                    "createExecutionRequest": {
                        "workflowId": params["workflowId"],
                        "timeWindowTimeZone": "America/New_York"
                    }
                },
                "schedule_expression": record["ScheduleExpression"],
                "rule_name": record["Name"],
                "rule_description": 'Rule migrated from v1 of AIOA'
            }
        except Exception:
            print(f"ERROR: skipping invalid record {record}")
            return [None, None, True]
        
        if "payload" in record:
            for i in record["payload"]:
                formatted_record["execution_request"]["createExecutionRequest"][i] = record["payload"][i] 
        else:
            workflow_details = self.get_scheduled_workflow(workflow_id=params["workflowId"], filepath=f'{os.environ["WRITE_PATH"]}/{DataConfig().data_files["wfm_workflows"]}')
            required_params = [
                "timeWindowStart", 
                "timeWindowEnd",
                "timeWindowType",
                "workflowExecutedDate"
            ]
            if workflow_details and "defaultPayload" in workflow_details.keys():
                for i in required_params:
                    if i in workflow_details["defaultPayload"].keys():
                        formatted_record["execution_request"]["createExecutionRequest"][i] = workflow_details["defaultPayload"][i]
                    else:
                        formatted_record["execution_request"]["createExecutionRequest"][i] = "<VALUE MISSING>"
                        invalid_flag = True
                for extra_param in workflow_details["defaultPayload"].keys():
                    if extra_param not in required_params:
                        formatted_record["execution_request"]["createExecutionRequest"][extra_param] = workflow_details["defaultPayload"][extra_param]
                        if type(formatted_record["execution_request"]["createExecutionRequest"][extra_param]) == bool:
                            formatted_record["execution_request"]["createExecutionRequest"][extra_param] = str(formatted_record["execution_request"]["createExecutionRequest"][extra_param]).lower()
            else:
                print(f"ERROR: missing workflow details for schedule {record}")
                return [None, None, True]
        
        if formatted_record["schedule_expression"].startswith("custom"):
            try:
                cron = self.convert_to_cron(custom=formatted_record["schedule_expression"])
                formatted_record["schedule_expression"] = cron
            except Exception:
                formatted_record["schedule_expression"] = "<CONVERT TO CRON>"
                invalid_flag = True
    
        return [formatted_record, invalid_flag, False]
    
    def format_records(self, record_type, read_fp, write_fp, invalid_fp):
        print(f'formatting {record_type} records and saving to filepath {write_fp}')
        
        try:
            with open(read_fp, 'r') as f:
                record_list = json.load(f)
        except Exception:
            print(f"skipping record type: {record_type}, {read_fp} filepath does not exists")
            return

        formatted_data = []
        invalid_data = []
        for record in record_list:
            if record_type == "customer":
                processed = self.format_customer_record(record)
            elif record_type == "workflow":
                processed = self.format_workflow_record(record)
            elif record_type == 'schedule':
                processed = self.format_schedule_record(record)
            
            if processed[2] == True:
                continue

            elif processed[1] == True:
                invalid_data.append(processed[0])
                continue

            formatted_data.append(processed[0])

        if len(formatted_data) > 0:
            with open(write_fp, 'w') as f:
                json.dump(formatted_data, f)

        if len(invalid_data) > 0:
            print(f'invalid {record_type} record(s) found. Check {invalid_fp} filepath')
            with open(invalid_fp, "w") as f:
                json.dump(invalid_data, f)

