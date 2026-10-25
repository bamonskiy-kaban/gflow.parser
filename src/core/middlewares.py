import asyncio
import logging
import os
from time import monotonic

from taskiq import TaskiqMiddleware

from core.database import engine
from core.heartbeat import TaskHeartbeat, current_task
from core.runtime import finish_task, start_task

log = logging.getLogger(__name__)


def terminate_worker():
    # Only after result persistence / best-effort cleanup. Taskiq supervisor replaces the child.
    logging.shutdown()
    os._exit(1)


class ResultMiddleware(TaskiqMiddleware):
    async def pre_execute(self, message):
        current_task.set(None)
        # A DB error here deliberately leaves the Redis Stream message unacknowledged.
        # The function has not begun; redelivery is not a function retry.
        item = await start_task(message.task_id, message.task_name.rsplit(":", 1)[-1])
        if item is not None:
            state = TaskHeartbeat(item, asyncio.current_task(), worker=True)
            current_task.set(state)
            state.start()
        return message

    async def post_execute(self, message, result):
        state = current_task.get()
        if state is None:
            return
        await state.stop()
        current_task.set(None)
        error = state.failure or (
            (str(result.error) or type(result.error).__name__) if result.is_err else None
        )
        try:
            await finish_task(
                message.task_id,
                None if result.is_err else result.return_value,
                error,
                monotonic() - state.started,
            )
        except Exception:
            log.exception(
                "RESULT_NOT_SAVED task_id=%s records_count=%s error=%r",
                message.task_id,
                state.progress.records_count,
                error or state.progress.error,
            )
            if not state.fatal:
                raise  # No ACK. The function itself is still never re-executed.
        finally:
            if state.fatal:
                log.critical("Fatal error; terminating worker task_id=%s", message.task_id)
                terminate_worker()

    async def shutdown(self):
        await engine.dispose()
