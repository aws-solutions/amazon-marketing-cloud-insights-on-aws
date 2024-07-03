# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import shutil
from dataclasses import dataclass

DATASET_PARAMETERS_JSON_FILEPATH = os.environ["DATASET_PARAMETERS_JSON_FILEPATH"]
MOCK_SCRIPTS_DIR = os.environ["MOCK_SCRIPTS_DIR"]
GLUE_DIR = os.environ["GLUE_DIR"]
DATALAKE_LAYERS_TRANSFORMATIONS_DIR = os.environ["DATALAKE_LAYERS_TRANSFORMATIONS_DIR"]


@dataclass
class TestDatasetParameters:
    dataset: str = "testdataset"
    pipeline: str = "insights"
    stage_a_transform: str = "test_dataset_light_transform"
    stage_b_transform: str = "test_dataset_heavy_transform"


@dataclass
class ScriptsFilePaths:
    mock_glue_script = f"{MOCK_SCRIPTS_DIR}/main.py"
    glue_dst = f"{GLUE_DIR}/{TestDatasetParameters.dataset}/main.py"
    mock_light_transform = f"{MOCK_SCRIPTS_DIR}/{TestDatasetParameters.stage_a_transform}.py"
    mock_heavy_transform = f"{MOCK_SCRIPTS_DIR}/{TestDatasetParameters.stage_b_transform}.py"
    stage_a_transforms_dst = f"{DATALAKE_LAYERS_TRANSFORMATIONS_DIR}/stage_a_transforms/{TestDatasetParameters.stage_a_transform}.py"
    stage_b_transforms_dst = f"{DATALAKE_LAYERS_TRANSFORMATIONS_DIR}/stage_b_transforms/{TestDatasetParameters.stage_b_transform}.py"


def read_dataset_config(json_filepath: str):
    if os.stat(json_filepath).st_size != 0:
        with open(json_filepath, "r") as output_file:
            current_config = json.load(output_file)
            return current_config
    return None


def write_dataset_config(json_filepath: str, config: dict):
    config_json_object = json.dumps(config, indent=4)
    with open(json_filepath, "w") as output_file:
        output_file.write(config_json_object)


def write_extended_dataset_config_to_json():
    current_dataset_config = read_dataset_config(DATASET_PARAMETERS_JSON_FILEPATH)
    test_dataset_config = {
        "dev": [
            {
                "dataset": TestDatasetParameters.dataset,
                "pipeline": TestDatasetParameters.pipeline,
                "config": {
                    "stage_a_transform": TestDatasetParameters.stage_a_transform,
                    "stage_b_transform": TestDatasetParameters.stage_b_transform
                }
            }
        ]
    }

    if current_dataset_config is None or len(current_dataset_config)==0:
        write_dataset_config(DATASET_PARAMETERS_JSON_FILEPATH, test_dataset_config)
    else:
        current_dataset_config["dev"].extend(test_dataset_config["dev"])
        write_dataset_config(DATASET_PARAMETERS_JSON_FILEPATH, current_dataset_config)


def add_glue_script(glue_src, glue_dst):
    os.makedirs(os.path.dirname(glue_dst), exist_ok=True)
    shutil.copy2(glue_src, glue_dst)


def add_datalake_transforms(stage_a_src, stage_a_dst, stage_b_src, stage_b_dst):
    shutil.copy2(stage_a_src, stage_a_dst)
    shutil.copy2(stage_b_src, stage_b_dst)


write_extended_dataset_config_to_json()
add_glue_script(ScriptsFilePaths.mock_glue_script, ScriptsFilePaths.glue_dst)
add_datalake_transforms(
    ScriptsFilePaths.mock_light_transform, ScriptsFilePaths.stage_a_transforms_dst,
    ScriptsFilePaths.mock_heavy_transform, ScriptsFilePaths.stage_b_transforms_dst)

