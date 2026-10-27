FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev
COPY common ./common
COPY gateway ./gateway
COPY orders ./orders
COPY inventory ./inventory
ENV PATH="/app/.venv/bin:$PATH"
# The service is chosen by the compose `command`, e.g. `uvicorn orders.app:app`.
