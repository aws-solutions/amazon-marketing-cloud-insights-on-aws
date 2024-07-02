# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
from configure_extended_dataset import read_dataset_config, write_dataset_config, TestDatasetParameters, \
    ScriptsFilePaths

DATASET_PARAMETERS_JSON_FILEPATH = os.environ["DATASET_PARAMETERS_JSON_FILEPATH"]
GLUE_DIR = os.environ["GLUE_DIR"]


def cleanup_extended_dataset_scripts():
    # Remove stage a and b transform scripts
    for transform_file in [ScriptsFilePaths.stage_a_transforms_dst, ScriptsFilePaths.stage_b_transforms_dst]:
        if os.path.exists(transform_file):
            os.remove(transform_file)
            print(f"\nDelete {transform_file} successfully")
        else:
            print(f"\n{transform_file} doesn't exist")

    # Remove GLue scripts
    shutil.rmtree(f"{GLUE_DIR}/{TestDatasetParameters.dataset}")


def cleanup_extended_dataset_configurations():
    current_dataset_config = read_dataset_config(DATASET_PARAMETERS_JSON_FILEPATH)
    if current_dataset_config is not None:
        try:
            print(f'\nDeleting test dataset parameters {TestDatasetParameters} from {DATASET_PARAMETERS_JSON_FILEPATH}')
            current_dataset_config['dev'] = [i for i in current_dataset_config['dev'] if
                                             i['dataset'] != TestDatasetParameters.dataset]
            if len(current_dataset_config['dev']) == 0:
                write_dataset_config(DATASET_PARAMETERS_JSON_FILEPATH, {})
            else:
                write_dataset_config(DATASET_PARAMETERS_JSON_FILEPATH, current_dataset_config)
        except Exception as e:
            print(f"Error deleting {TestDatasetParameters.dataset} config in {DATASET_PARAMETERS_JSON_FILEPATH}: {e}")


print('\n*Cleaning extended dataset config*')
cleanup_extended_dataset_configurations()
cleanup_extended_dataset_scripts()
