"""Domain entities and one durable execution table; no task mixins."""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class CaseTable(Base):
    __tablename__ = "cases"
    id: Mapped[int] = mapped_column(primary_key=True)
    short_name: Mapped[str] = mapped_column(String(64), unique=True)
    display_name: Mapped[str] = mapped_column(String(255))
    evidences: Mapped[list["EvidenceTable"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True
    )


class EvidenceTable(Base):
    __tablename__ = "evidences"
    __table_args__ = (
        UniqueConstraint("case_id", "filename", name="uq_evidence_case_filename"),
        CheckConstraint("status IN ('uploading', 'processing', 'completed', 'partial', 'failed')"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    case_id: Mapped[int] = mapped_column(ForeignKey("cases.id", ondelete="CASCADE"), index=True)
    uid: Mapped[str] = mapped_column(String(101), unique=True)
    filename: Mapped[str] = mapped_column(String(255))
    storage_path: Mapped[str] = mapped_column(String(512))
    status: Mapped[str] = mapped_column(String(16), default="uploading", index=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    error: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    hostname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    os: Mapped[str | None] = mapped_column(String(32), nullable=True)
    arch: Mapped[str | None] = mapped_column(String(255), nullable=True)
    version: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ips: Mapped[str | None] = mapped_column(String(255), nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(255), nullable=True)
    language: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tasks: Mapped[list["TaskTable"]] = relationship(
        lazy="selectin", cascade="all, delete-orphan", passive_deletes=True
    )


class TaskTable(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        UniqueConstraint("evidence_id", "name", "function_name", name="uq_task_evidence_operation"),
        CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed')"),
        CheckConstraint("name IN ('upload', 'acquire_target_info', 'process_function')"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    evidence_id: Mapped[int] = mapped_column(ForeignKey("evidences.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(32))
    function_name: Mapped[str] = mapped_column(String(255), default="")
    status: Mapped[str] = mapped_column(String(16), default="queued", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    exec_time: Mapped[float] = mapped_column(default=0)
    records_count: Mapped[int] = mapped_column(BigInteger, default=0)
    bytes_received: Mapped[int] = mapped_column(BigInteger, default=0)
    error: Mapped[str | None] = mapped_column(String(2048), nullable=True)


Index(
    "ix_tasks_live_heartbeat",
    TaskTable.heartbeat_at,
    postgresql_where=TaskTable.status == "running",
    sqlite_where=TaskTable.status == "running",
)
