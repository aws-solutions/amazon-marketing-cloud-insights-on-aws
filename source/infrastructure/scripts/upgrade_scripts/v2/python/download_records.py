# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from modules.v1_interface import V1Interface
from modules.v1_resources import V1Resources
from modules.data_config import DataConfig

ddk_path = os.environ["DDK_PATH"]
write_path = os.environ["WRITE_PATH"]

v1_interface = V1Interface()
v1_resources = V1Resources(ddk=ddk_path)
data_config = DataConfig()

# SAVE customer records from v1 tps customer config table
print("\nSTARTING DOWNLOAD PROCESS FOR CUSTOMER RECORDS...\n")
v1_interface.save_records(
    table_name=v1_resources.tps_customer_config_table, 
    filepath=f'{write_path}/{data_config.data_files["tps_customer_config"]}'
    )

# SAVE workflow records from v1 wfm workflow table
print("\nSTARTING DOWNLOAD PROCESS FOR WORKFLOW RECORDS...\n")
v1_interface.save_records(
    table_name=v1_resources.wfm_workflows_table, 
    filepath=f'{write_path}/{data_config.data_files["wfm_workflows"]}'
    )

# SAVE workflow schedules from v1 wfm schedules table
print("\nSTARTING DOWNLOAD PROCESS FOR SCHEDULE RECORDS...\n")
v1_interface.save_records(
    table_name=v1_resources.wfm_schedules_table, 
    filepath=f'{write_path}/{data_config.data_files["wfm_schedules"]}',
    )

