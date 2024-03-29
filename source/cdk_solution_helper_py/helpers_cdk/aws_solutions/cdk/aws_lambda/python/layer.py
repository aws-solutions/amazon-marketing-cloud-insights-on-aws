# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Union, List
from uuid import uuid4

from aws_cdk import BundlingOptions, DockerImage, AssetHashType
from aws_cdk.aws_lambda import LayerVersion, Code
from constructs import Construct

from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonBundling

DEPENDENCY_EXCLUDES = ["*.pyc"]


class SolutionsPythonLayerVersion(LayerVersion):
    """Handle local packaging of layer versions"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        requirements_path: Path,
        libraries: Union[List[Path], None] = None,
        **kwargs,
    ):  # NOSONAR
        self.scope = scope
        self.construct_id = construct_id
        self.requirements_path = requirements_path

        # validate requirements path
        if not self.requirements_path.is_dir():
            raise ValueError(
                f"requirements_path {self.requirements_path} must not be a file, but rather a directory containing Python requirements in a requirements.txt file, pipenv format or poetry format"
            )

        libraries = [] if not libraries else libraries
        for lib in libraries:
            if lib.is_file():
                raise ValueError(
                    f"library {lib} must not be a file, but rather a directory"
                )

        bundling = SolutionsPythonBundling(
            self.requirements_path, libraries=libraries, install_path="python"
        )

        kwargs["code"] = self._get_code(bundling)

        # initialize the LayerVersion
        super().__init__(scope, construct_id, **kwargs)

    def _get_code(self, bundling: SolutionsPythonBundling) -> Code:
        # create the layer version locally
        code_parameters = {
            "path": str(self.requirements_path),
            "asset_hash_type": AssetHashType.CUSTOM,
            "asset_hash": uuid4().hex,
            "exclude": DEPENDENCY_EXCLUDES,
        }

        code = Code.from_asset(
            bundling=BundlingOptions(
                image=DockerImage.from_registry(
                    "scratch"
                ),  # NEVER USED - FOR NOW ALL BUNDLING IS LOCAL
                command=["not_used"],
                entrypoint=["not_used"],
                local=bundling,
            ),
            **code_parameters,
        )

        return code
