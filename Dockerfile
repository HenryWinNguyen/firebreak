FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY firebreak ./firebreak
COPY config ./config
RUN uv sync --locked --no-dev
EXPOSE 8000
CMD ["uv", "run", "--no-dev", "uvicorn", "firebreak.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
