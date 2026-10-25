#!/bin/sh
set -eu
# One task per child; one fetched message. Streams retain an unacknowledged
# message if a child dies before claiming its DB task.
exec taskiq worker worker.tasks:broker --max-async-tasks 1 \
    --max-tasks-per-child 1 --max-prefetch 1 --ack-type when_saved --workers "${WORKERS:-2}"
