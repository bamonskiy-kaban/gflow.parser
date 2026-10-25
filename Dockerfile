FROM python:3.13-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock ./

FROM base AS api
RUN uv sync --frozen --no-dev --extra api
COPY src/core ./src/core/
COPY src/api ./src/api/
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS worker
RUN uv sync --frozen --no-dev --extra worker
COPY src/core ./src/core/
COPY src/worker ./src/worker/
COPY scripts/worker.sh ./scripts/worker.sh
ENV WORKERS=2
CMD ["sh", "/app/scripts/worker.sh"]
