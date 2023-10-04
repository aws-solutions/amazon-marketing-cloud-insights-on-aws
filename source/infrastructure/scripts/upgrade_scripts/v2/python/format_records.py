# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import os

from modules.v1_interface import V1Interface
from modules.data_config import DataConfig

read_path = os.environ["READ_PATH"]
success_path = os.environ["SUCCESS_PATH"]
failed_path = os.environ["FAILED_PATH"]

v1_interface = V1Interface()
data_config = DataConfig()

print("\nSTARTING FORMAT PROCESS FOR CUSTOMER RECORDS...\n")
v1_interface.format_records(
    record_type="customer", 
    read_fp=f'{read_path}/{data_config.data_files["tps_customer_config"]}', 
    write_fp=f'{success_path}/{data_config.data_files["tps_customer_config"]}',
    invalid_fp=f'{failed_path}/{data_config.data_files["tps_customer_config"]}'
    )

print("\nSTARTING FORMAT PROCESS FOR WORKFLOW RECORDS...\n")
v1_interface.format_records(
    record_type="workflow", 
    read_fp=f'{read_path}/{data_config.data_files["wfm_workflows"]}', 
    write_fp=f'{success_path}/{data_config.data_files["wfm_workflows"]}',
    invalid_fp=f'{failed_path}/{data_config.data_files["wfm_workflows"]}'
    )

print("\nSTARTING FORMAT PROCESS FOR SCHEDULE RECORDS...\n")
v1_interface.format_records(
    record_type='schedule',
    read_fp=f'{read_path}/{data_config.data_files["wfm_schedules"]}', 
    write_fp=f'{success_path}/{data_config.data_files["wfm_schedules"]}',
    invalid_fp=f'{failed_path}/{data_config.data_files["wfm_schedules"]}'
)

