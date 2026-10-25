"""One dedicated Dissect thread, leaving the event loop free for live checkpoints."""

import asyncio
import logging
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger(__name__)


class ParserThread:
    def __init__(self) -> None:
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dissect")
        self.pending: asyncio.Future | None = None

    async def run[T](self, function: Callable[..., T], *args) -> T:
        future = asyncio.get_running_loop().run_in_executor(self.pool, function, *args)
        # Consume late exceptions if the task is cancelled while Dissect is blocked.
        future.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        self.pending = future
        try:
            return await asyncio.shield(future)
        finally:
            if future.done():
                self.pending = None

    async def close(self, iterator: Generator[bytes, None, None]) -> None:
        if self.pending is not None and not self.pending.done():
            # Queue close after the in-flight read; never close a running generator.
            log.warning("Dissect call still running; cleanup deferred to parser thread")
            self.pool.submit(self._close_later, iterator)
        else:
            await self.run(iterator.close)

    @staticmethod
    def _close_later(iterator: Generator[bytes, None, None]) -> None:
        try:
            iterator.close()
        except Exception:
            log.exception("Deferred parser cleanup failed")

    def shutdown(self) -> None:
        # No queued parsing work: one task and one sequential read at a time.
        self.pool.shutdown(wait=False)
