import asyncio
import logging
from collections.abc import Generator

from dissect.target.target import Target
from flow.record import Record, iter_timestamped_records

from core.broker import broker
from core.heartbeat import task_context
from core.models import FunctionProcessingResult, TargetInfoAcquireResult
from core.storage import target_path
from worker.config import (
    BOOTSTRAP_SERVERS,
    KAFKA_LOG_INTERVAL,
    KAFKA_RETRY_DELAY,
    KAFKA_TIMEOUT,
    MAX_BATCH_BYTES,
    MAX_BATCH_EVENTS,
    MAX_EVENT_BYTES,
    TOPIC,
)
from worker.event_writer import FatalKafkaError, KafkaEventWriter
from worker.parser_thread import ParserThread
from worker.serialization import JsonRecordPackerWrapper

log = logging.getLogger(__name__)


def close_target(target):
    for disk in target.disks:
        disk.close()


def read_info(relative_path: str) -> TargetInfoAcquireResult:
    target = Target.open(target_path(relative_path))
    try:
        return TargetInfoAcquireResult(
            hostname=target.hostname,
            domain=getattr(target, "domain", None),
            os=target.os,
            arch=getattr(target, "architecture", None),
            version=getattr(target, "version", None),
            ips=", ".join(getattr(target, "ips", []) or []),
            timezone=getattr(target, "timezone", None),
            language=", ".join(getattr(target, "language", []) or []),
        )
    finally:
        close_target(target)


@broker.task
async def acquire_target_info() -> TargetInfoAcquireResult:
    state = task_context()
    parser = ParserThread()
    try:
        return await parser.run(read_info, state.item.storage_path)
    except asyncio.CancelledError:
        asyncio.current_task().uncancel()
        raise RuntimeError(state.failure or "Metadata collection interrupted") from None
    finally:
        parser.shutdown()


def events(uid: str, relative_path: str, function_name: str) -> Generator[bytes, None, None]:
    target = Target.open(target_path(relative_path))
    try:
        _, function = target.get_function(function_name)
        packer = JsonRecordPackerWrapper(uid, function_name, target.os)
        for record in function():
            if isinstance(record, Record):
                for event in iter_timestamped_records(record):
                    yield packer.pack(event)
    finally:
        close_target(target)


def read_batch(iterator) -> tuple[list[bytes], str | None, bool]:
    batch, size = [], 0
    try:
        while len(batch) < MAX_BATCH_EVENTS and size < MAX_BATCH_BYTES:
            event = next(iterator)
            if len(event) > MAX_EVENT_BYTES:
                log.warning(f"Serialized event size [{len(event)}] exceeds limit: [{MAX_EVENT_BYTES}] bytes. Skipping")
                continue
            batch.append(event)
            size += len(event)
    except StopIteration:
        return batch, None, True
    except Exception as exc:
        log.exception("Parser stopped")
        return batch, str(exc) or type(exc).__name__, True
    return batch, None, False


@broker.task
async def process_function() -> FunctionProcessingResult:
    state = task_context()
    item = state.item
    writer = KafkaEventWriter(
        BOOTSTRAP_SERVERS,
        TOPIC,
        timeout=KAFKA_TIMEOUT,
        retry_delay=KAFKA_RETRY_DELAY,
        log_interval=KAFKA_LOG_INTERVAL,
        on_ack=lambda count: setattr(state.progress, "records_count", count),
    )
    parser, iterator, error = ParserThread(), None, None
    try:
        if not BOOTSTRAP_SERVERS or not TOPIC:
            raise FatalKafkaError("BOOTSTRAP_SERVERS and TOPIC are required")
        iterator = events(item.uid, item.storage_path, item.function_name)
        async with writer:
            finished = False
            while not finished:
                batch, error, finished = await parser.run(read_batch, iterator)
                state.progress.error = error
                for event in batch:
                    await writer.write_event(event)
                # Publish the valid tail even if parsing failed partway through a batch.
                await writer.flush()
                batch.clear()
    except asyncio.CancelledError:
        asyncio.current_task().uncancel()
        reason = state.failure or "Worker interrupted"
        error = f"{error}; {reason}" if error else reason
    except Exception as exc:
        if isinstance(exc, FatalKafkaError):
            state.fatal = True
        message = str(exc) or type(exc).__name__
        error = f"{error}; {message}" if error else message
        log.exception("Function failed task_id=%s acked=%d", item.id, writer.records_acked)
    finally:
        if iterator is not None and not state.fatal:
            try:
                await parser.close(iterator)
            except Exception as exc:
                log.exception("Parser cleanup failed")
                error = error or str(exc) or type(exc).__name__
        parser.shutdown()
    state.progress.error = error
    log.info("Function finished task_id=%s acked=%d error=%r", item.id, writer.records_acked, error)
    return FunctionProcessingResult(records_count=writer.records_acked, processing_error=error)
