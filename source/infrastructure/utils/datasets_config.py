# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from pathlib import Path
from dataclasses import dataclass
from aws_lambda_powertools import Logger

logger = Logger(service='get-datasets-parameter-from-config-file', level="INFO")


@dataclass
class SDLFDatasetParameters:
    dataset: str
    pipeline: str
    stage_a_transform: str
    stage_b_transform: str


class DatasetsConfigs:
    def __init__(self, environment_id, config_file_path) -> None:
        with open(config_file_path) as f:
                self._config_file = json.load(f)
        self._configs = self._config_file.get(environment_id, [])

    @property
    def dataset_configs(self):
        parameters = []
        for config in self._configs:
            if config:
                try:
                    dataset_parameters = SDLFDatasetParameters(
                        dataset=config["dataset"],
                        pipeline=config["pipeline"],
                        stage_a_transform=config["config"]["stage_a_transform"],
                        stage_b_transform=config["config"]["stage_b_transform"],
                    )
                    parameters.append(dataset_parameters)
                except KeyError as e:
                    logger.error("Missing configurations for the dataset parameters in file datasets_parameters.json.")
                    raise e
        return parameters
