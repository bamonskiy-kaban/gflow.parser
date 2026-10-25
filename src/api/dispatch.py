"""Task rows are the publication queue. Started functions are never resubmitted."""

import asyncio
import logging
from datetime import timedelta
from time import monotonic

from sqlalchemy import func, or_, select

from api.stubs import acquire_target_info_stub, process_function_stub
from core.broker import broker
from core.config import DISPATCH_RECHECK, DISPATCH_TIMEOUT
from core.database import async_session_maker
from core.runtime import expire_tasks
from core.tables import TaskTable

log = logging.getLogger(__name__)
WORKER_TASKS = {"acquire_target_info": acquire_target_info_stub, "process_function": process_function_stub}
last_error_log = float("-inf")


def log_unavailable(message, *args):
    global last_error_log
    if monotonic() - last_error_log >= 30:
        last_error_log = monotonic()
        log.exception(message, *args)


async def dispatch_tasks(limit: int = 100) -> int:
    sent = 0
    for _ in range(limit):
        async with async_session_maker.begin() as session:
            now = await session.scalar(select(func.now()))
            task = await session.scalar(
                select(TaskTable)
                .where(
                    TaskTable.status == "queued",
                    TaskTable.name != "upload",
                    or_(
                        TaskTable.dispatched_at.is_(None),
                        TaskTable.dispatched_at < now - timedelta(seconds=DISPATCH_RECHECK),
                    ),
                )
                .order_by(TaskTable.created_at, TaskTable.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if task is None:
                break
            handler = WORKER_TASKS[task.name]
            try:
                async with asyncio.timeout(DISPATCH_TIMEOUT):
                    await handler.kicker().with_task_id(task.id).kiq()
            except Exception:
                log_unavailable("Task publication unavailable task_id=%s", task.id)
                break  # Release the connection; the next tick retries publication only.
            task.dispatched_at = now
            sent += 1
    return sent


async def maintain_tasks():
    from redis.asyncio import Redis

    client = Redis(connection_pool=broker.connection_pool)
    try:
        # Re-create the native group after Redis state loss. Re-publication of queued
        # rows also covers a worker creating the group before this API observes the loss.
        await broker.startup()
        groups = await client.xinfo_groups(broker.queue_name)
        if len(groups) == 1:
            group = groups[0]
            pending = await client.xpending(broker.queue_name, broker.consumer_group_name)
            cutoff = pending["min"] or group["last-delivered-id"]
            if cutoff not in {b"0-0", "0-0"}:
                await client.xtrim(broker.queue_name, minid=cutoff, approximate=True)
    finally:
        await client.aclose()


async def maintenance_loop():
    while True:
        try:
            await expire_tasks()
            await maintain_tasks()
            await dispatch_tasks()
        except Exception:
            log_unavailable("Task maintenance temporarily unavailable")
        await asyncio.sleep(1)
