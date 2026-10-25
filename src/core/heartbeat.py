import asyncio
import logging
from contextvars import ContextVar
from dataclasses import dataclass, field
from time import monotonic

from core.config import HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT
from core.models import Progress, WorkItem
from core.runtime import checkpoint

log = logging.getLogger(__name__)


@dataclass
class TaskHeartbeat:
    item: WorkItem
    owner: asyncio.Task
    progress: Progress = field(default_factory=Progress)
    started: float = field(default_factory=monotonic)
    last_sent: float = field(default_factory=monotonic)
    failure: str | None = None
    fatal: bool = False
    ticker: asyncio.Task | None = None
    worker: bool = False

    def start(self):
        self.ticker = asyncio.create_task(self.run(), name=f"heartbeat:{self.item.id}")

    async def run(self):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            try:
                if await checkpoint(self.item, self.progress, monotonic() - self.started):
                    self.last_sent = monotonic()
                    continue
                self.failure = "Task already finished or heartbeat expired"
            except Exception:
                log.exception("Heartbeat unavailable task_id=%s", self.item.id)
                if monotonic() - self.last_sent < HEARTBEAT_TIMEOUT:
                    continue
                self.failure = "Worker lost DB contact"
            self.fatal = self.worker  # Stop native parsing too when the worker loses ownership.
            self.owner.cancel()
            return

    async def stop(self):
        if self.ticker is not None:
            self.ticker.cancel()
            await asyncio.gather(self.ticker, return_exceptions=True)


current_task: ContextVar[TaskHeartbeat | None] = ContextVar("current_task", default=None)


def task_context() -> TaskHeartbeat:
    from taskiq import NoResultError

    state = current_task.get()
    if state is None:
        raise NoResultError  # Duplicate or obsolete message, never another execution.
    return state
