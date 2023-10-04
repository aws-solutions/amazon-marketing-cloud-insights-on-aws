# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from .__version__ import __version__, __title__  # noqa: F401;
from .event import EventReasonEnum  # noqa: F401;
from .client import OctagonClient  # noqa: F401;
from .artifact import Artifact  # noqa: F401;

name = "octagon"

# Suppress boto3 logging
logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
