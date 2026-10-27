.PHONY: install fmt lint typecheck test check owned up down logs

install:
	uv sync

fmt:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy firebreak scripts

test:
	uv run pytest -q

# make test-ingest -> tests/ingest
test-%:
	uv run pytest -q tests/$*

check: lint typecheck test

# Merge gate G2 (ownership) for the current branch
owned:
	uv run python scripts/check_owned_paths.py

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100
