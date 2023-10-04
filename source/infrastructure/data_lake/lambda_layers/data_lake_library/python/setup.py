# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
from pathlib import Path

import setuptools

VERSION_RE = re.compile(r"\#\# \[(?P<version>.*)\]", re.MULTILINE)  # NOSONAR


def get_version():
    """
    Detect the solution version from the changelog. Latest version on top.
    """
    changelog = open(Path(__file__).resolve().parent.parent.parent.parent.parent.parent.parent / "CHANGELOG.md").read()
    versions = VERSION_RE.findall(changelog)
    if not len(versions):
        raise ValueError("use the standard semver format in your CHANGELOG.md")
    build_version = versions[0]
    print(f"Build Version: {build_version}")
    return build_version


setuptools.setup(
    name="datalake-library",
    version=get_version(),
    description=
    "The Serverless Data Lake Framework (SDLF) is a collection of reusable artifacts aimed at accelerating the delivery of enterprise data lakes on AWS",
    author="Amazon Web Services",
    url="https://aws.amazon.com/solutions/implementations",
    license="Apache License 2.0",
    packages=setuptools.find_namespace_packages(),
    python_requires=">=3.9",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
