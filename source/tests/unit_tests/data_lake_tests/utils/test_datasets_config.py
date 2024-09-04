# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# ###############################################################################
# PURPOSE:
#   * Unit test for Utils/DataSetsConfig.
# USAGE:
#   ./run-unit-tests.sh --test-file-name utils/test_datasets_config.py
###############################################################################

import json
from data_lake.utils.datasets_config import DatasetsConfigs, SDLFDatasetParameters
from unittest.mock import MagicMock, mock_open, patch


def test_data_sets_configs_cls():
    config_file_path = "/some_configs.json"
    environment_id = "dev_12345"
    configs = [
        {
            "dataset": "1234",
            "pipeline": "some_pipeline",
            "config": {
                "stage_a_transform": "testa",
                "stage_b_transform": "testb"
            }
        }
    ]
    with patch(
            "builtins.open", mock_open(read_data=json.dumps({environment_id: configs}))
        ) as mock_file:

        parameters = DatasetsConfigs(environment_id, config_file_path).dataset_configs
    mock_file.assert_called_with(config_file_path)
    assert parameters == [SDLFDatasetParameters(dataset='1234', pipeline='some_pipeline', stage_a_transform='testa', stage_b_transform='testb')]