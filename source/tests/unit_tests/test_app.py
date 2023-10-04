# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Infrastructure/app.py.
# USAGE:
#   ./run-unit-tests.sh --test-file-name test_app.py
###############################################################################


import sys
from unittest.mock import patch


@patch("aws_cdk.App")
@patch("amc_insights.amc_insights_stack.AMCInsightsStack")
def test_build_app(amc_insight_mock, app_cdk_mock):
    sys.path.insert(0, "./infrastructure")
    with patch("app.__name__", "__main__"):
        from app import build_app
        build_app()
        amc_insight_mock.assert_called_once()
        app_cdk_mock.assert_called_once()