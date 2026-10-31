.PHONY: install fmt lint typecheck test check owned up down logs migrate eval

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

# AGENT=<dir under evals/recordings> or AGENT=all (every committed agent dir).
# Replay exits non-zero only for stale recordings or errors; graded misses exit 0.
AGENT ?= baseline-local
RECORDINGS_DIR ?= evals/recordings

eval:  ## Replay-mode evals over recorded scenarios ($0); AGENT=all replays every agent dir
	@agents="$(AGENT)"; \
	if [ "$$agents" = "all" ]; then \
		agents=$$(ls -d $(RECORDINGS_DIR)/*/ 2>/dev/null | xargs -n1 basename); \
		if [ -z "$$agents" ]; then echo "no recordings under $(RECORDINGS_DIR)" >&2; exit 1; fi; \
	fi; \
	for a in $$agents; do \
		echo "== replay $$a"; \
		uv run python -m firebreak.evals run --mode replay --agent $$a --recordings-dir $(RECORDINGS_DIR) || exit 1; \
	done; \
	uv run python -m firebreak.evals report

migrate:  ## Apply DB migrations to FIREBREAK_DATABASE_URL
	uv run alembic -c firebreak/db/alembic.ini upgrade head
