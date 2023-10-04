# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

class DataConfig():
    def __init__(self):

        self.data_files = {
            'tps_customer_config': 'customers.json',
            'wfm_workflows': 'workflows.json',
            'wfm_schedules': 'schedules.json'
        }
        