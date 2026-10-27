---
id: T-0xx
title: <short title>
milestone: M?
track: target | core | agents | evals | dashboard
model: sonnet | opus
risk: normal | high
depends_on: []            # task ids that must be merged to main first
owned_paths:              # the ONLY paths this task may edit
  - path/one/
status: pending           # lead-only: pending|ready|in-progress|in-review|rework|merged|escalated
---

## Goal
One or two sentences: what exists when this is done.

## Mandatory reading (before writing any code)
- `CLAUDE.md`
- `docs/CONTRACTS.md` §x
- `firebreak/models/<file>.py`

## Patterns to mirror
- `<existing file>`: how errors, logging and tests are done there

## Definition of done
- [ ] Behavior 1 (observable)
- [ ] Behavior 2
- [ ] Tests cover: <edge cases>
- [ ] No silent failures: every caught exception is re-raised, logged with context, or turned into a typed error
- [ ] PROGRESS.md entry added in the PR

## Verify command
```bash
make test-<area>
```

## Out of scope
- Things a builder might be tempted to do but shouldn't
