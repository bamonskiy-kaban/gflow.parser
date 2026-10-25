"""Bounded streaming uploads; all filesystem calls run outside the API event loop."""

import asyncio
import hashlib
import os
from functools import partial
from urllib.parse import unquote

from fastapi import HTTPException, Request

from core.config import MAX_UPLOAD_BYTES
from core.heartbeat import TaskHeartbeat
from core.storage import target_path


def filename_from_header(value: str | None) -> str:
    try:
        name = unquote(value or "", errors="strict")
    except UnicodeError:
        raise HTTPException(400, "Invalid UTF-8 filename") from None
    if (
        not name
        or name in {".", "..", ".upload.part"}
        or len(name.encode("utf-8")) > 255
        or any(c in name for c in "/\\")
        or any(ord(c) < 32 or ord(c) == 127 for c in name)
    ):
        raise HTTPException(400, "Provide a plain filename in the filename header (URL-encoded UTF-8)")
    return name


async def disk(function, *args, **kwargs):
    # Finish the current write before close/unlink even if the HTTP request is cancelled.
    operation = asyncio.create_task(asyncio.to_thread(partial(function, *args, **kwargs)))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        await operation
        raise


async def save_upload(request: Request, state: TaskHeartbeat) -> tuple[int, str]:
    path = await disk(target_path, state.item.storage_path)
    await disk(path.parent.mkdir, parents=True, exist_ok=False)
    temporary = path.with_name(".upload.part")
    file = None
    size, checksum = 0, hashlib.sha256()
    try:
        file = await disk(temporary.open, "xb")
        async for body in request.stream():
            # A large ASGI frame must not create a large hashing/write pause in the event loop.
            for offset in range(0, len(body), 1024 * 1024):
                chunk = memoryview(body)[offset : offset + 1024 * 1024]
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(413, "Upload exceeds MAX_UPLOAD_BYTES")

                def write():
                    file.write(chunk)
                    checksum.update(chunk)

                await disk(write)
                state.progress.bytes_received = size
        if not size:
            raise HTTPException(400, "Empty upload")
        await disk(file.flush)
        await disk(os.fsync, file.fileno())
        await disk(file.close)
        file = None
        await disk(temporary.replace, path)

        # Persist the directory entry as well as the file before publishing work.
        def sync_directory():
            descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

        await disk(sync_directory)
        return size, checksum.hexdigest()
    finally:
        if file is not None:
            await disk(file.close)
        await disk(temporary.unlink, missing_ok=True)
