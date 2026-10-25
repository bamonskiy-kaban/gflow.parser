from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ReadModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class CaseCreate(BaseModel):
    short_name: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    display_name: str = Field(min_length=1, max_length=255)


class CaseRead(CaseCreate, ReadModel):
    id: int


class TaskRead(ReadModel):
    id: str
    name: str
    function_name: str
    status: str
    created_at: datetime
    dispatched_at: datetime | None
    heartbeat_at: datetime | None
    finished_at: datetime | None
    exec_time: float
    records_count: int
    bytes_received: int
    error: str | None


class EvidenceRead(ReadModel):
    id: int
    case_id: int
    uid: str
    filename: str
    status: str
    size_bytes: int | None
    sha256: str | None
    created_at: datetime
    error: str | None
    hostname: str | None
    domain: str | None
    os: str | None
    arch: str | None
    version: str | None
    ips: str | None
    timezone: str | None
    language: str | None
    tasks: list[TaskRead]
