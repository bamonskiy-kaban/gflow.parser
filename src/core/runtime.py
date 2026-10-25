"""All task transitions and automatic fan-out happen in short DB transactions."""

from datetime import timedelta

from sqlalchemy import func, select, update

from core.config import HEARTBEAT_TIMEOUT
from core.database import async_session_maker
from core.functions import FUNCTIONS
from core.models import FunctionProcessingResult, Progress, TargetInfoAcquireResult, WorkItem
from core.tables import EvidenceTable, TaskTable

TERMINAL = {"completed", "partial", "failed"}


def bounded(model, values):
    for name, value in values.items():
        limit = getattr(model.__table__.c[name].type, "length", None)
        if limit and isinstance(value, str):
            values[name] = value[:limit]
    return values


async def locked_task(session, task_id):
    eid = await session.scalar(select(TaskTable.evidence_id).where(TaskTable.id == task_id))
    if eid is None:
        return None, None
    # All terminal transitions use the same lock order: Evidence -> Task.
    evidence = await session.scalar(
        select(EvidenceTable)
        .where(EvidenceTable.id == eid)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    task = await session.scalar(
        select(TaskTable)
        .where(TaskTable.id == task_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return evidence, task


async def start_task(task_id: str, name: str) -> WorkItem | None:
    async with async_session_maker.begin() as session:
        evidence, task = await locked_task(session, task_id)
        if (
            name not in {"acquire_target_info", "process_function"}
            or task is None
            or evidence is None
            or task.name != name
            or task.status != "queued"
            or evidence.status != "processing"
        ):
            return None
        task.status = "running"
        task.heartbeat_at = await session.scalar(select(func.now()))
        item = WorkItem(
            task.id, evidence.id, task.name, task.function_name, evidence.uid, evidence.storage_path
        )
    return item


async def checkpoint(item: WorkItem, progress: Progress, elapsed: float) -> bool:
    async with async_session_maker.begin() as session:
        now = await session.scalar(select(func.now()))
        changed = await session.execute(
            update(TaskTable)
            .where(
                TaskTable.id == item.id,
                TaskTable.status == "running",
                TaskTable.heartbeat_at >= now - timedelta(seconds=HEARTBEAT_TIMEOUT),
            )
            .values(
                **bounded(
                    TaskTable,
                    dict(
                        heartbeat_at=now,
                        exec_time=elapsed,
                        records_count=progress.records_count,
                        bytes_received=progress.bytes_received,
                        error=progress.error,
                    ),
                )
            )
        )
    return bool(changed.rowcount)


async def aggregate_evidence(session, evidence):
    await session.flush()
    states = list(
        await session.scalars(
            select(TaskTable.status).where(
                TaskTable.evidence_id == evidence.id, TaskTable.name == "process_function"
            )
        )
    )
    if any(state in {"queued", "running"} for state in states):
        return
    if states:
        failed = states.count("failed")
        acked = await session.scalar(
            select(func.sum(TaskTable.records_count)).where(
                TaskTable.evidence_id == evidence.id, TaskTable.name == "process_function"
            )
        )
        evidence.status = (
            "completed" if not failed else ("partial" if "succeeded" in states or acked else "failed")
        )
        evidence.error = f"{failed} of {len(states)} functions failed" if failed else None


async def finish_task(task_id: str, result, error: str | None, elapsed: float) -> bool:
    async with async_session_maker.begin() as session:
        evidence, task = await locked_task(session, task_id)
        if task is None or evidence is None or task.status != "running":
            return False
        if task.name == "acquire_target_info" and error is None:
            if not isinstance(result, TargetInfoAcquireResult):
                error = "Invalid metadata result"
            else:
                for key, value in bounded(EvidenceTable, result.model_dump()).items():
                    setattr(evidence, key, value)
                functions = sorted(name for name, info in FUNCTIONS.items() if result.os in info)
                if not functions:
                    error = f"No configured functions for platform: {result.os}"
                else:
                    session.add_all(
                        TaskTable(evidence_id=evidence.id, name="process_function", function_name=name)
                        for name in functions
                    )
        elif task.name == "process_function":
            if isinstance(result, FunctionProcessingResult):
                task.records_count = result.records_count
                error = error or result.processing_error
            elif error is None:
                error = "Invalid function result"
        task.error = error[:2048] if error else None
        task.exec_time = elapsed
        task.finished_at = await session.scalar(select(func.now()))
        task.status = "failed" if error else "succeeded"
        if task.name in {"upload", "acquire_target_info"} and error:
            evidence.status, evidence.error = "failed", task.error
        elif task.name == "process_function":
            await aggregate_evidence(session, evidence)
    return True


async def finish_upload(task_id: str, size: int, sha256: str, elapsed: float) -> bool:
    async with async_session_maker.begin() as session:
        evidence, task = await locked_task(session, task_id)
        if task is None or evidence is None or task.status != "running" or evidence.status != "uploading":
            return False
        evidence.size_bytes, evidence.sha256, evidence.status = size, sha256, "processing"
        task.bytes_received, task.exec_time, task.status = size, elapsed, "succeeded"
        task.finished_at = await session.scalar(select(func.now()))
        session.add(TaskTable(evidence_id=evidence.id, name="acquire_target_info"))
    return True


async def expire_tasks() -> int:
    async with async_session_maker() as session:
        now = await session.scalar(select(func.now()))
        ids = list(
            await session.scalars(
                select(TaskTable.id)
                .where(
                    TaskTable.status == "running",
                    TaskTable.heartbeat_at < now - timedelta(seconds=HEARTBEAT_TIMEOUT),
                )
                .limit(100)
            )
        )
    expired = 0
    for task_id in ids:
        async with async_session_maker.begin() as session:
            evidence, task = await locked_task(session, task_id)
            now = await session.scalar(select(func.now()))
            if (
                task is None
                or task.status != "running"
                or task.heartbeat_at >= now - timedelta(seconds=HEARTBEAT_TIMEOUT)
            ):
                continue
            task.status, task.finished_at = "failed", now
            previous = f"{task.error}; " if task.error else ""
            task.error = (previous + "Heartbeat expired; last checkpoint preserved")[:2048]
            if task.name == "process_function":
                await aggregate_evidence(session, evidence)
            else:
                evidence.status, evidence.error = "failed", task.error
            expired += 1
    return expired
