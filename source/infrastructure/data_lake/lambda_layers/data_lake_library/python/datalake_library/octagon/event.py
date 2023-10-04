# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
import datetime
from enum import Enum
from .utils import throw_none_or_empty, get_local_date, get_timestamp_iso, get_ttl, throw_if_false
from ..commons import init_logger


class EventReasonEnum(Enum):
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


class EventAPI:
    def __init__(self, client):
        self.logger = init_logger()
        self.client = client
        self.dynamodb = client.dynamodb
        self.events_ttl = client.config.get_events_ttl()
        self.events_table = client.dynamodb.Table(client.config.get_events_table())

    def create_event(self, reason, comment, component_name=None, event_details=None):

        throw_if_false(self.client.is_pipeline_set(), "Pipeline execution is not yet assigned")

        throw_none_or_empty(str(reason), "No reason specified")
        throw_none_or_empty(comment, "No comment specified")

        self.logger.debug(f"event create_event() called. Reason : {reason}, comment: {comment}")

        if isinstance(reason, EventReasonEnum):
            reason_str = reason.value
        else:
            reason_str = reason

        # Put to DDB

        item = {}
        current_time = datetime.datetime.utcnow()
        local_date_iso = get_local_date()
        utc_time_iso = get_timestamp_iso(current_time)
        # Add extra fields
        item["id"] = str(uuid.uuid4())
        item["pipeline_execution_id"] = self.client.pipeline_execution_id
        item["pipeline"] = self.client.pipeline_name
        item["reason"] = reason_str
        item["comment"] = comment
        item["timestamp"] = utc_time_iso
        item["date"] = local_date_iso
        item["date_and_reason"] = item["date"] + "#" + item["reason"]
        if component_name:
            item["component"] = component_name
        if event_details:
            item["details"] = event_details

        if self.events_ttl != 0:
            item["ttl"] = get_ttl(self.events_ttl)
        # Save to DDB
        self.events_table.put_item(Item=item)
        return item["id"]

    def get_event(self, id):
        return self.events_table.get_item(Key={"id": id}, ConsistentRead=True)["Item"]
