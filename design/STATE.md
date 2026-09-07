# State at 2026-09-04, end of session

Worktree `/workspaces/bluesky/.claude/worktrees/runengine-split`, branch
`runengine-split`. **Rebased onto `origin/main` `a6a9ecfb6`**, tree clean, nothing
pushed -- coretl `runengine-split` is stale, both because the rebase gave every
commit a new SHA and because of the twelve commits added since. coretl PR #3 is a
private staging area for the upstream PR Tom will eventually make; force-pushing
it needs no ceremony.

## Read these three, in this order

1. `/workspaces/bluesky/.claude/notes/runengine-split-interface.md` -- every
   decision with its reasoning, the working order, and what is still open. This is
   the important one; it is untracked, so it does not travel with the branch.
2. `design/classes.py` -- the agreed interface, one line per item. No prose: it all
   lives in the notes.
3. `design/questions-for-tom-caswell.md` -- restructured into open and closed
   questions.

## What happened this session

First the rebase onto main, which brought in #1923
(`Subscribable.subscribe_reading`) and #2052 (the `CallbackRegistry` classmethod
fix -- PR2 was carrying its own copy, and main's version won). Then a long design
pass over `classes.py` with Tom. Then implementation, additively, of everything
that pass settled.

**Twelve commits of implementation are in.** The notes file lists them with SHAs
and says what each pinning test caught. In short: the three pin commits, then the
permit simplification, dispatcher chaining, the `emit` collapse, `identity`, the
`md` snapshot, the prologue move, deleting `RunEngine.request_suspend`, and the
two documentation pages.

One pinning test found a live bug rather than a designed change:
`RE.clear_suspenders()` could not empty `RE.suspenders`, because the executor
still held a stale copy of the session's set -- the copy the design had already
decided to delete. Caswell's beam-down escape hatch was broken on the branch, and
nothing else would have noticed.

**Nothing has been pushed.**

## The plan

PR1/PR2 is abandoned: one branch, one series of ~12 commits, each individually
reviewable.

| # | commit | additive to main? |
|---|---|---|
| 0a | pin suspension semantics: two conditions at once, trip while paused, re-trip inside `sleep` | yes -- **done** |
| 0b | pin the session/plan boundary: `Msg` install/remove in a plan, `clear_suspenders` while paused then resume, `RE.md` mid-plan, monitor callback thread | yes -- **done** |
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

Rows 1-10 describe how the work will be *presented*, not how it was built. The
tree currently holds it as thirteen additive commits; the restructure into this
shape is step 4 of the working order below and has not started.

Row 10 is now complete: the privatisation sweep is done, in one commit. On
`PlanExecutor`, `env`, `loop`, `rewind`, `unbound_default_commands`,
`command_registry`, `permit`, `dispatcher`, `exception` and `reason` are now
private; `run_engine.py`'s `_FORWARDS_WITH_CALLERS` and
`_FORWARDS_WITHOUT_KNOWN_CALLERS` maps were repointed at the new names in the
same commit. `PlanSession.subscribe`/`unsubscribe` and its four hook-forwarding
properties went too.

It is the `exception`/`exit_status`/`reason` **trio** after all, settled by what
the RunEngine actually needs rather than by what the design documents said. The
RunEngine reads none of the three on the executor -- only `interrupted` and
`run_start_uids`, which stay public. The trio's public face is `RunEngineResult`,
which `result()` returns and which carries exactly those three fields, so
privatising them on the executor takes nothing away.

`PlanExecutor.__init__` now takes `permit`, `hooks` and `dispatcher` as required
positional arguments, matching the design. There was exactly one direct
`PlanExecutor(...)` call in the whole tree -- `make_executor` itself -- so the
`None` defaults were unreachable branches, and every caller in src, tests and
docs already goes through `session.make_executor`.

`PlanSession.__init__` went the other way, from ten arguments to five. The rule,
set by Tom: a setting the RunEngine exposes a **property with a setter** for is a
plain attribute, because changing it after construction is public interface; a
setting written once at `RunEngine.__init__` with no setter anywhere is a
constructor argument. That keeps `md`, `loop`, `log`, `run_bundler_cls` and
`identity` as arguments, and makes attributes of `scan_id_source`,
`preprocessors`, `md_validator` and `md_normalizer` -- joining
`record_interruptions`, `strict_pre_declare` and `rewindable`, which already
were. `md` is in both halves, and has to be.

`on_pause` is the one exception, decided against the rule: it is written once and
has no property, but `hooks` is how all four hooks are reached, and one of the
four arriving by another route would say otherwise. It is assigned after
construction. `PlanSession()` still constructs with no arguments at all.

`Permit.withhold` no longer defaults `justification` to the empty string.

Two docs commits, which ship in the PR -- **both written** (`98a82e780`):

- `docs/architecture.rst` -- autodoc/autosummary over the real classes with
  narrative between, on what pokes what. Autodoc rather than a copied listing so it
  cannot drift, which is what the one-line-per-item rule bought.
- `docs/headless.rst` -- driving a session and executor with no RunEngine, every
  sample tested. Free: `addopts` already carries `--doctest-glob="*.rst"` and CI
  runs bare `pytest` from the root, so rst doctests are collected. Samples must
  emit no un-ignored warning (`filterwarnings = ["error", ...]`) and wrap async in
  `asyncio.run`.

`pin-scan-id-and-rewindable` held one pinning commit written against main. It has
been rebased onto current main and cherry-picked in as `7d0004e4d`. Its subject
uses a lowercase `test:` where the repo uses `TST:` -- normalise it at the
restructure step.

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

Split it around the SIGINT flake, which aborts the run part way -- add
``--ignore=src/bluesky/tests/test_run_engine.py`` and run that file separately
with ``-k "not sigint"``.

The docs now carry doctests, which the usual recipe does not collect because it
passes a path. Run ``pytest docs/headless.rst`` as well; CI gets them for free,
since it runs bare ``pytest`` from the root and ``addopts`` carries
``--doctest-glob="*.rst"``.

Building the docs needs ``PYTHONPATH=$PWD/src`` too. Without it sphinx imports
the venv's installed bluesky, which has no ``permits`` module, and autodoc fails
on every class in ``architecture.rst``.

Put `/venv/bin` on `PATH` or the ruff pre-commit hook fails with "Executable `ruff`
not found".

`test_watch_finished_before_set_return_when_set_finishes` **now fails most of the
time on this machine**, and the tolerance is the cause. Measured 2026-09-07: three
failures in five runs of the whole file, uncontended, always around 0.273-0.276 s
against a 0.2 s +/- 0.05 assertion -- and identically on `07985e557`, the commit
before the sweep, so it is not attributable to any of this work. It passes every
time when run alone. The machine is simply slower today than during the
implementation session, which is exactly the condition the note below predicted
for CI. Expect it red there, and fix the tolerance rather than the split.

The original note, kept because it is what was actually measured at the time:

`test_watch_finished_before_set_return_when_set_finishes` is worth watching on
CI. It asserts a 0.2 s wall time to within 0.05 s and failed three times during
the implementation session, always while another pytest or sphinx run was
competing for the machine. It then passed 26 consecutive runs on the branch --
five quiet, three under synthetic CPU load, and three on each of the six
intermediate commits -- and `main` passed 11 under the same conditions. So it
could not be pinned on any change, and could not be provoked deliberately
either. If it appears on CI, which is roughly twice as slow as here, suspect the
tolerance rather than the split; but it had not failed before this work, so do
not write it off.

`test_sigint_during_suspender_active` and its neighbours are a **known flake Tom
Caswell is investigating**. They hang rather than fail, on CI too -- ubuntu jobs
sitting at an hour on PR2's runs are this, not us. Ignore them; do not chase.
