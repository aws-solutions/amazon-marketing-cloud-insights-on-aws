# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json

class V1Resources():
    def __init__(self, ddk):
        self.ddk = json.load(open(ddk))

        for i in self.ddk['environments'].keys():
            if i == 'cicd':
                continue
            self.env = i

        self.account = self.ddk['environments'][self.env]['account']
        self.region = self.ddk['environments'][self.env]['region']
        self.prefix = self.ddk['environments'][self.env]['resource_prefix']

        self.team = self.ddk['environments'][self.env]['data_pipeline_parameters']['team']
        self.pipeline = self.ddk['environments'][self.env]['data_pipeline_parameters']['pipeline']
        self.dataset = self.ddk['environments'][self.env]['data_pipeline_parameters']['dataset']
        self.org = self.ddk['environments'][self.env]['data_pipeline_parameters']['org']
        self.app = self.ddk['environments'][self.env]['data_pipeline_parameters']['app']

        self.tps_customer_config_table = f"tps-{self.team}-CustomerConfig-{self.env}"
        self.wfm_workflows_table = f"wfm-{self.team}-AMCWorkflows-{self.env}"
        self.wfm_schedules_table = f"wfm-{self.team}-AMCWorkflowSchedules-{self.env}"

        