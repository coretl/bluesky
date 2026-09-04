# State at 2026-09-04, end of session

Worktree `/workspaces/bluesky/.claude/worktrees/runengine-split`, branch
`runengine-split`. **Rebased onto `origin/main` `a6a9ecfb6`**, tree clean, nothing
pushed -- coretl `runengine-split` is stale by 43 commits because the rebase gave
every commit a new SHA. coretl PR #3 is a private staging area for the upstream PR
Tom will eventually make; force-pushing it needs no ceremony.

## Read these three, in this order

1. `/workspaces/bluesky/.claude/notes/runengine-split-interface.md` -- every
   decision with its reasoning, the working order, and what is still open. This is
   the important one; it is untracked, so it does not travel with the branch.
2. `design/classes.py` -- the agreed interface, one line per item. No prose: it all
   lives in the notes.
3. `design/questions-for-tom-caswell.md` -- restructured into open and closed
   questions.

## What happened this session

The rebase onto main, which brought in #1923 (`Subscribable.subscribe_reading`) and
#2052 (the `CallbackRegistry` classmethod fix -- PR2 was carrying its own copy of
that fix, and main's version won). Then a long design pass over `classes.py` with
Tom. Nothing has been implemented yet: every commit this session is docs.

Local suite after the rebase: **1862 passed**, 2 known environmental failures
(`test_buffering::test_callback_logging_exceptions`,
`test_tiled_writer::test_imports_raise_warnings`) plus the psutil errors.
`mypy src` clean outside `_vendor`.

## The plan

PR1/PR2 is abandoned: one branch, one series of ~12 commits, each individually
reviewable.

| # | commit | additive to main? |
|---|---|---|
| 0a | pin suspension semantics: two conditions at once, trip while paused, re-trip inside `sleep`, no-checkpoint abort | yes |
| 0b | pin the session/plan boundary: `Msg` install/remove in a plan, `clear_suspenders` while paused then resume, `RE.md` mid-plan, monitor callback thread | yes |
| 1 | `permits.py` -- `Permit`, `Suspension`, tests | yes |
| 2 | give `Dispatcher` a parent | yes |
| 3 | suspenders withhold a permit instead of holding the RunEngine | |
| 4 | move plan execution into `plan_executor.py` -- **pure move**, nothing renamed | |
| 5 | split into `PlanSession` + `PlanExecutor`, with `PlanEnvironment`, `PlanHooks`, `identity` | |
| 6 | chain permits and dispatchers per plan: plan-local suspenders and subscribers | |
| 7 | one suspension per episode; pre-plans as they fire, post-plans in reverse | |
| 8 | don't swallow a condition that goes bad as the plan starts | |
| 9 | collapse `emit`/`emit_sync` into one synchronous `emit` | |
| 10 | narrow the public surface: privatise, delete `request_suspend` and `run_engine_cls` | |

Plus two docs commits, which ship in the PR:

- `docs/architecture.rst` -- autodoc/autosummary over the real classes with
  narrative between, on what pokes what. Autodoc rather than a copied listing so it
  cannot drift, which is what the one-line-per-item rule bought.
- `docs/headless.rst` -- driving a session and executor with no RunEngine, every
  sample tested. Free: `addopts` already carries `--doctest-glob="*.rst"` and CI
  runs bare `pytest` from the root, so rst doctests are collected. Samples must
  emit no un-ignored warning (`filterwarnings = ["error", ...]`) and wrap async in
  `asyncio.run`.

`pin-scan-id-and-rewindable` is an existing branch with one pinning commit already
written against main (+55 lines to `test_run_engine.py`). Fold it into 0b.

## Working order, set by Tom

1. Build everything **additively** on this branch. Do not restructure while the
   design is still moving.
2. Prove it locally: full suite, `mypy src`, ruff, and `pytest docs/` -- the usual
   recipe passes a path, so it does not collect the doctests.
3. Push to coretl, check CI.
4. Split into the series.
5. Force-push it.

The `design/` commits do not go upstream. Drop them at the restructure step.

## Still open

1. Caswell's Q1, the monitor trampoline. Unanswered, and orthogonal -- nothing in
   the series waits on it. Collapsing `emit`/`emit_sync` is *not* the trampoline: it
   does not change which thread a monitor callback runs on.
2. The unanswered half of Caswell's clear_suspenders note: whether
   `Msg('clear_suspenders')` should exist when it could only cover a plan's own, and
   whether an overridable permit is the shutdown control he meant.
3. #1806 rewrites the same `SuspenderBase.install` we do. Build on `main` and
   rebase once it merges -- do **not** base the branch on `suspender-signature`,
   which is still in review and may be force-pushed. The conflict is one method in
   one file, and resolves the same way whenever: their loop-marshalling stays,
   sourced from the permit's loop instead of the RunEngine's.

## Test recipe

    PYTHONPATH=$PWD/src /venv/bin/python -m pytest src/bluesky/tests/ -q -p no:randomly \
        --ignore=src/bluesky/tests/test_streams.py

Put `/venv/bin` on `PATH` or the ruff pre-commit hook fails with "Executable `ruff`
not found".

`test_sigint_during_suspender_active` and its neighbours are a **known flake Tom
Caswell is investigating**. They hang rather than fail, on CI too -- ubuntu jobs
sitting at an hour on PR2's runs are this, not us. Ignore them; do not chase.
