# State at 2026-09-04, end of session

Worktree `/workspaces/bluesky/.claude/worktrees/runengine-split`, branch
`runengine-split`, tracking `coretl/runengine-split` = **coretl/bluesky PR #3**.
Branch was force-pushed to start from `origin/main` (`6a82217aa`); the previous
head is `f81005717` if anything is needed back.

Read `classes.py` (the target interface), `suspension-permits.md` (the spec) and
`questions-for-tom-caswell.md` (six questions, five answered inline by Caswell).

## Commits, oldest first

| commit | what |
|---|---|
| `698c5be39` | **PR1**: suspension through a `Permit`, on `main` |
| `487166aa9` | the design docs (not for upstream) |
| `951affa34` | two more questions for Caswell |
| `482e5098b` | acting on Caswell's answers |
| `49477897d` | docs: pre-plans must be idempotent, api_changes |
| `7e2875b96` | fix: don't swallow a trip that lands as the plan starts |
| `072f6bac6` | tests for the two deprecated routes |
| `6358cee44` | **PR2**: split RunEngine into PlanSession + PlanExecutor |

**PR1 for upstream is `698c5be39` plus `482e5098b`, `49477897d`, `7e2875b96`,
`072f6bac6`.** PR2 is `6358cee44`. The docs commits do not go upstream.

## UNCOMMITTED — finish this first

`src/bluesky/plan_executor.py` and `src/bluesky/run_engine.py` are modified and
**not committed**. The change reconciles the built classes with `classes.py`:

- `Hooks` renamed to `PlanHooks`
- `adopt_suspender` deleted (no callers; a running plan's permit is a child of
  the session's, so nothing needs telling)
- the executor's `install_suspender` / `remove_suspender` became
  `_install_suspender_now` / `_remove_suspender_now`, reachable only through
  `Msg`, per the rule that plan-local things are only reachable from inside the
  plan
- `RunEngine.remove_suspender` no longer also calls the executor's

Targeted tests pass (84: test_plan_executor, test_permits, test_suspenders),
ruff and format clean. **The full suite has not been run since these edits** --
that run was interrupted. Run it, then commit and push.

## Test recipe

    PYTHONPATH=$PWD/src /venv/bin/python -m pytest src/bluesky/tests/ -q -p no:randomly \
        --ignore=src/bluesky/tests/test_streams.py

Expect **3 failures**, all environmental and unrelated: `test_buffering.py::
test_callback_logging_exceptions`, `test_tiled_writer.py::
test_imports_raise_warnings`, `test_zmq.py::test_proxy_script`, plus 22 psutil
errors. Last complete run before the uncommitted edits: **1855 passed**.

Runs abort intermittently part-way (~1380 passed, ~120s) on the pre-existing
~21%-flaky SIGINT test. That is not a regression -- re-run, or split the suite
with `--ignore=src/bluesky/tests/test_run_engine.py` and run that file alone.

`mypy src` is part of CI lint and ruff alone will not catch it. It reports 5
errors in `src/bluesky/_vendor`, which CI excludes; anything outside `_vendor`
is ours.

## Deviations from `classes.py` — the thing Tom asked to be told

**Forced by Caswell's answer to Q4** (pre-plans run when their condition fires,
post-plans in reverse), which needs per-reason access the merged `suspension`
cannot give:

- `Permit.reasons` is back, alongside `suspension`
- `Permit.wait_changed()` added, so the supervisor notices conditions joining an
  episode already in progress

**Deliberate, and worth a look:**

- `SuspenderBase.install` also accepts a `RunEngine`, with a
  `DeprecationWarning`, doing a durable install -- Caswell asked for exactly
  this (isinstance, warn, delegate).
- `RunEngine.request_suspend` is deprecated rather than deleted; it delegates to
  `_suspend_until`. Caswell said "make private".
- `RunEngine.permit` is public. `suspender.RE` is gone.
- `Permit.__init__` needs a real loop -- `grant(after=...)` schedules a timer.

**Still not reconciled** (would need another pass):

- `PlanSession` has more than `classes.py` lists: `clear_suspenders`, and
  `msg_hook` / `state_hook` / `waiting_hook` / `on_pause` forwards over
  `session.hooks`. `RunEngine` forwards through them, so removing them means
  pointing `RunEngine` at `session.hooks` directly.
- `PlanExecutor` still exposes `block_run`, `permit_run`,
  `deferred_pause_requested`, `emit`, `env`, `loop`, `request_suspend`,
  `result`, `rewind`, `suspenders`, `unbound_default_commands`. Most are there
  because `RunEngine` drives them. `classes.py` lists only `run`, `state`,
  `resumable`, `rewindable_flag`, the five lifecycle verbs and
  `run_start_uids` -- so either `classes.py` is missing what a RunEngine
  legitimately needs, or these want to move behind it. **This is the biggest
  open gap and worth Tom's opinion before churning it.**

## Decisions already taken (in the spec, with reasoning)

Answered by Tom Cobb: a trip while paused suspends on resume; `Msg('install_
suspender')` is plan-local; no-checkpoint mid-plan aborts and pushes nothing;
no weak references in `Permit` -- a permit only ever looks upwards.

Answered by Caswell, in `questions-for-tom-caswell.md`: `RE.suspenders` reports
session and executors (done); `ignore_callback_exceptions` should become init
state on the RunEngine (**not done**); pre/post plan ordering (done);
`install(RE)` warn-and-delegate (done); `request_suspend` private (done).

**Q1, the monitor trampoline, is the one Caswell has not answered.** Note the
old branch collapsed `emit`/`emit_sync` into one synchronous `emit`; I reverted
that, because it rode on an unrelated `Subscribable.subscribe` ->
`subscribe_reading` protocol rename which is not part of this work.

## What is left

1. Run the full suite, commit and push the uncommitted reconciliation.
2. `ignore_callback_exceptions` as init state on the RunEngine (Caswell's Q3).
3. Decide the `PlanExecutor` public surface against `classes.py` (above).
4. Caswell's notes at the top of the questions doc are unaddressed and marked
   `??` by him: whether command behaviour and `md` should be frozen at the
   launch of execute. Both are `PlanEnvironment` questions.
5. `docs/api_changes.rst` covers PR1 only; PR2 needs its own entry.
