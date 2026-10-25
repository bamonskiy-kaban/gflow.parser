from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from core.config import DATABASE_URL

# Each process creates its own pool. Connections are not shared between workers.
options = {"pool_pre_ping": True}
if DATABASE_URL.startswith("postgresql+asyncpg:"):
    options.update(pool_size=5, max_overflow=5, pool_timeout=10)
    options["connect_args"] = {"timeout": 5, "command_timeout": 10}
engine = create_async_engine(DATABASE_URL, **options)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)
