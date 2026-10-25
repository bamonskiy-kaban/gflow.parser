"""Bounded Kafka delivery. Retry transport failures, never restart the parser."""

import asyncio
import logging
from contextlib import AbstractAsyncContextManager
from math import isfinite
from time import monotonic

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from worker.config import MAX_BATCH_BYTES, MAX_BATCH_EVENTS

log = logging.getLogger(__name__)


class FatalKafkaError(RuntimeError):
    """Save the partial result and terminate this worker process."""


def retryable(error: BaseException) -> bool:
    return isinstance(error, TimeoutError) or isinstance(error, KafkaError) and error.retriable


class KafkaEventWriter(AbstractAsyncContextManager):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        *,
        batch_size=MAX_BATCH_EVENTS,
        batch_bytes=MAX_BATCH_BYTES,
        timeout=20,
        retry_delay=5,
        log_interval=30,
        on_ack=None,
    ):
        if any(
            not isfinite(value) or value <= 0
            for value in (batch_size, batch_bytes, timeout, retry_delay, log_interval)
        ):
            raise ValueError("Writer limits must be positive finite numbers")
        self.servers, self.topic = bootstrap_servers, topic
        self.batch_size, self.batch_bytes = batch_size, batch_bytes
        self.timeout, self.retry_delay, self.log_interval = timeout, retry_delay, log_interval
        self.on_ack = on_ack
        self.producer = None
        self.buffer: list[bytes] = []
        self.buffer_bytes = 0
        self.records_acked = 0
        self._last_log = float("-inf")

    def log_wait(self, phase, error=None):
        if monotonic() - self._last_log >= self.log_interval:
            self._last_log = monotonic()
            log.warning(
                "Waiting for Kafka topic=%s phase=%s acked=%s error=%r",
                self.topic,
                phase,
                self.records_acked,
                error,
            )

    async def close_producer(self, *, recovering=False):
        producer, self.producer = self.producer, None
        if producer is not None:
            try:
                if recovering:
                    # stop() drains admitted events. During recovery that drain must
                    # wait for Kafka too, rather than turn an outage into a fatal timeout.
                    self.log_wait("drain before reconnect")
                    await producer.stop()
                else:
                    async with asyncio.timeout(self.timeout):
                        await producer.stop()
            except asyncio.CancelledError as exc:
                raise FatalKafkaError("Kafka client cleanup interrupted") from exc
            except Exception as exc:
                # Never accumulate clients whose shutdown failed.
                raise FatalKafkaError(f"Kafka client cleanup failed: {exc}") from exc

    async def connect(self):
        while self.producer is None:
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.servers,
                    enable_idempotence=True,
                    acks="all",
                    max_batch_size=65536,
                    linger_ms=5,
                    max_request_size=1024 * 1024,
                    request_timeout_ms=max(1, int(self.timeout * 1000)),
                    retry_backoff_ms=max(1, int(self.retry_delay * 1000)),
                )
                async with asyncio.timeout(self.timeout):
                    await self.producer.start()
            except BaseException as exc:
                await self.close_producer()
                if isinstance(exc, asyncio.CancelledError):
                    raise
                if not retryable(exc):
                    raise FatalKafkaError(str(exc) or type(exc).__name__) from exc
                self.log_wait("connect", exc)
                await asyncio.sleep(self.retry_delay)

    async def __aenter__(self):
        await self.connect()
        return self

    async def write_event(self, event: bytes):
        if self.producer is None:
            raise RuntimeError("Writer is not open")
        if self.buffer and self.buffer_bytes + len(event) > self.batch_bytes:
            await self.flush()
        self.buffer.append(event)
        self.buffer_bytes += len(event)
        if len(self.buffer) >= self.batch_size:
            await self.flush()

    async def deliver(self, event):
        # Normal outages are retried internally with the same idempotent producer.
        await self.producer.send_and_wait(self.topic, event)
        self.records_acked += 1
        if self.on_ack:
            self.on_ack(self.records_acked)

    async def flush(self):
        if not self.buffer:
            return
        remaining, self.buffer = self.buffer, []
        self.buffer_bytes = 0
        while remaining:
            deliveries = {asyncio.create_task(self.deliver(event)): event for event in remaining}
            pending = set(deliveries)
            error = None
            try:
                while pending and error is None:
                    done, pending = await asyncio.wait(
                        pending, timeout=self.log_interval, return_when=asyncio.FIRST_EXCEPTION
                    )
                    errors = [task.exception() for task in done if task.exception() is not None]
                    error = next((exc for exc in errors if not retryable(exc)), None) or next(
                        iter(errors), None
                    )
                    if pending:
                        self.log_wait("delivery", error)
            except BaseException:
                for task in deliveries:
                    task.cancel()
                await asyncio.gather(*deliveries, return_exceptions=True)
                raise
            if error is None:
                return
            for task in pending:
                task.cancel()
            await asyncio.gather(*deliveries, return_exceptions=True)
            remaining = [
                event
                for task, event in deliveries.items()
                if task.cancelled() or task.exception() is not None
            ]
            errors = [task.exception() for task in deliveries if not task.cancelled() and task.exception()]
            fatal = next((exc for exc in errors if not retryable(exc)), None)
            if fatal is not None:
                raise FatalKafkaError(str(fatal) or type(fatal).__name__) from fatal
            self.log_wait("reconnect", error)
            await self.close_producer(recovering=True)
            await asyncio.sleep(self.retry_delay)
            await self.connect()

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                await self.flush()
        finally:
            self.buffer.clear()
            self.buffer_bytes = 0
            await self.close_producer()
