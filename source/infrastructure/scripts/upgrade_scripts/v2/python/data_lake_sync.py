# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os 
import sys
import json
from datetime import datetime

from modules.v2_interface import V2Interface

write_path = sys.argv[1]
tps_customer_config_table = os.environ["V2_TPS_CUSTOMER_CONFIG"]
data_lake_trigger_lambda = os.environ["V2_DATA_LAKE_TRIGGER_LAMBDA"]

v2_interface = V2Interface()

# get all configured amc instance buckets
bucket_list = v2_interface.get_instance_buckets(
    table_name=tps_customer_config_table
)

# get all object keys for each amc instance bucket
data_map = []
for bucket in bucket_list:
    keys = v2_interface.get_object_keys(bucket_name=bucket)
    data_map.append(
            {
            'bucket_name': bucket,
            'data': keys
            }
        )

# trigger data lake lambda for each object key
response_list = {}
for instance in data_map:

    bucket_name = instance['bucket_name']
    response_list[bucket_name] = {}

    print(f'\nstarting data lake sync for bucket: {bucket_name}\n')

    for object_key in instance['data']:
        trigger_event = {
            "detail-type": "AWS API Call via CloudTrail",
            "detail": {
                "eventName": "PutObject",
                "requestParameters": {
                    "bucketName": bucket_name,
                    "key": object_key
                }
            },
            "time": datetime.now().isoformat().split('.')[0] + "Z"
        }

        response_code = v2_interface.send_trigger_event(
            lambda_name = data_lake_trigger_lambda,
            event=trigger_event
        )
        
        if response_code not in response_list[bucket_name].keys():
            response_list[bucket_name][response_code] = []
        response_list[bucket_name][response_code].append(object_key)

# write results to json file locally
results_file = write_path + "/results.json"
with open(results_file, 'w') as f:
    json.dump(response_list, f)
    