from flow.record import Record, JsonRecordPacker
from typing import Any

import orjson


EXCLUDE_FIELDS = ["_recorddescriptor", "_source", "_classification", "_version", "_type", "_generated"]


class JsonRecordPackerWrapper:
    def __init__(self, uid: str, function_name: str, os: str):
        self.uid = uid
        self.func_name = function_name
        self.packer = JsonRecordPacker()
        self.os = os

    def pack_obj(self, obj: Any):
        result = self.packer.pack_obj(obj)
        if isinstance(obj, Record) and isinstance(result, dict):
            result["__gf_meta"] = {
                "os": self.os,
                "uid": self.uid,
                "function": self.func_name,
                "record_type": "event",
                "event_type": result["_recorddescriptor"][0]
            }
            for field in EXCLUDE_FIELDS:
                result.pop(field)
        return result

    def pack(self, obj: Any):
        return orjson.dumps(obj, default=self.pack_obj)
