from dataclasses import dataclass

from pydantic import BaseModel


class TargetInfoAcquireResult(BaseModel):
    hostname: str | None = None
    domain: str | None = None
    os: str
    arch: str | None = None
    version: str | None = None
    ips: str = ""
    timezone: str | None = None
    language: str = ""


class FunctionProcessingResult(BaseModel):
    records_count: int
    processing_error: str | None = None


@dataclass(frozen=True)
class WorkItem:
    id: str
    evidence_id: int
    name: str
    function_name: str
    uid: str
    storage_path: str


@dataclass
class Progress:
    records_count: int = 0
    bytes_received: int = 0
    error: str | None = None
