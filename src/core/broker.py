from taskiq_redis import RedisStreamBroker
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from core.config import REDIS_URL
from core.middlewares import ResultMiddleware

# One consumer group; count=1 avoids stranding a fetched batch when a child exits.
broker = RedisStreamBroker(
    url=REDIS_URL,
    queue_name="gflow.jobs.v02",
    consumer_group_name="gflow.workers.v02",
    consumer_id="0",
    xread_count=1,
    unacknowledged_batch_size=1,
    idle_timeout=60000,
    unacknowledged_lock_timeout=10,
    retry=Retry(NoBackoff(), 0),
).with_middlewares(ResultMiddleware())
