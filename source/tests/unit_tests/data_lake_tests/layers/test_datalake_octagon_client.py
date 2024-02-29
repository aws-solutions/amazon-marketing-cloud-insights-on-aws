# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from data_lake.lambda_layers.data_lake_library.python.datalake_library.octagon.client import OctagonClient

def test_octagon_client():
    client = OctagonClient()
    client.with_sns_topic("sns_topic")
    assert client.sns_topic == "sns_topic"
    client.with_run_lambda(True)
    assert client.run_in_lambda == True
    client.with_run_fargate(True)
    assert client.run_in_fargate == True
    client.with_region("region")
    assert client.region == "region"
    client.with_profile("profile")
    assert client.profile == "profile"
    client.with_config("config")
    assert client.configuration_file == "config"
    client.with_meta("meta")
    assert client.metadata_file == "meta"
    client.with_configuration_instance("configuration_instance", "resource_prefix")
    assert client.configuration_instance == "configuration_instance"
    assert client.resource_prefix == "resource_prefix"
