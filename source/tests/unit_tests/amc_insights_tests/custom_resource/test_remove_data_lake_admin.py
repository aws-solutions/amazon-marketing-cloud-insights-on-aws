# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
###############################################################################
# PURPOSE:
#   * Unit test for remove_data_lake_admin.py
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/custom_resource/test_remove_data_lake_admin.py
###############################################################################

import os
import boto3
from moto import mock_aws
  
@mock_aws
def test_on_delete():
    from amc_insights.custom_resource.lakeformation_settings.lambdas.remove_data_lake_admin import on_delete

    ## TEST 1
    # set up 2 existing lakeformation admin accounts
    lakeformation_client = boto3.client("lakeformation", region_name=os.environ["AWS_REGION"])
    lakeformation_client.put_data_lake_settings(DataLakeSettings={
            "DataLakeAdmins": [
                {"DataLakePrincipalIdentifier": "MAIN_ADMIN"},
                {"DataLakePrincipalIdentifier": "SOLUTION_ADMIN"}
            ]
        }
    )
    # assert that our function only removes the admin account deployed by the solution
    resource_properties = {
        "ResourceProperties" : {
            "ADMIN_ROLE_LIST" : [
                "SOLUTION_ADMIN"
            ]
        }
    }
    on_delete(resource_properties, None)
    check = lakeformation_client.get_data_lake_settings()
    assert check['DataLakeSettings']['DataLakeAdmins'] == [{'DataLakePrincipalIdentifier': 'MAIN_ADMIN'}]

    ## TEST 2
    # set lakeformation with 0 admin accounts
    lakeformation_client = boto3.client("lakeformation", region_name=os.environ["AWS_REGION"])
    lakeformation_client.put_data_lake_settings(DataLakeSettings={
            "DataLakeAdmins": []
        }
    )
    # assert that our function still runs successfully
    resource_properties = {
        "ResourceProperties" : {
            "ADMIN_ROLE_LIST" : [
                "SOLUTION_ADMIN"
            ]
        }
    }
    on_delete(resource_properties, None)
    check = lakeformation_client.get_data_lake_settings()
    assert check['DataLakeSettings']['DataLakeAdmins'] == []
