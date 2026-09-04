# Suspension permits

Interfaces in `classes.py`. This is why they are that shape.

Two PRs. **First, permits on `main`**: one `Permit` owned by the `RunEngine`, no
parent. That alone collapses simultaneous trips, deletes `_ev`, `__make_event`,
`__set_event` and `get_futures`, drops the `state.is_running` check made on the
device's thread, and moves the pre-plan wait into the run loop. **Then the
session/executor split** adds the parent chain, and with it the plan-local half.

So two decisions below are second-PR only, because they need a plan's own permit
to exist: `Msg('install_suspender')` being plan-local, and anything about the
chain. Everything else lands first.

## The model

A `Permit` is permission to run. It holds reasons, keyed by whoever raised
them, and is granted exactly when no reason stands. Each reason carries a
justification and the pre/post plans to run around the wait.

It reports them merged, as a single `Suspension`, rather than handing out the
reasons themselves. Merging is chain-order-dependent, so it belongs to the
permit; and one reason and five then look identical to everything downstream.

Permits form a chain. A session holds one; each plan gets
`Permit("plan", loop, parent=session.permit)`. A child is withheld whenever it or anything
above it is withheld. So *durable* and *plan-local* stop being two mechanisms:
a suspender installed on the session permit holds up every plan, one installed
on a plan's permit holds up that plan, and the difference is the argument to
`install`.

A suspender's only collaborator is a Permit. It does not know about the
session, the executor, the RunEngine, or whether anything is running. Its whole
job is to withhold while its signal reads as bad and grant when it recovers.

## What the executor does

One object to consult, before the plan and during it:

```python
await self.permit.wait_granted()          # prologue: nothing to rewind to yet
task = spawn(self._supervise())
try:
    ...run the plan...
finally:
    task.cancel()

async def _supervise(self):
    while True:
        await self.permit.wait_withheld()
        ...push Msg('_start_suspender', self.permit.suspension)...
        await self.permit.wait_granted()
```

`_start_suspender` is unchanged from main: record the interruption into every
open run, stop every object ever `set()`, `obj.pause()` on devices, rewind to
the last checkpoint, then pre-plan, wait, post-plan, replay.

## Decisions

**The permit decides whether to suspend and why; it does not perform the
suspension.** Awaiting a permit in the run loop would stop the plan where it
stood and resume it there, losing the rewind — a scan suspended mid-flight
would keep the points it took while the beam was down.

**The prologue and the suspension are the same await at two positions.** Before
the first message there is no checkpoint, so the only possible response is to
wait; after it, the response is a full suspension. Positional, not stateful.

**One suspension per episode is the supervisor's position in its own loop.** A
second condition tripping while it is parked on `wait_granted()` extends the
wait and cannot start a second rewind. Nothing has to remember whether the
episode was already announced.

**The child watches the parent; the parent knows nothing of children.** A
back-link would have to be weak or explicitly detached, or a finished plan's
permit is retained for the life of the session. The child's watcher needs a
primitive that wakes on *any* transition — `asyncio.Event` cannot signal the
cleared edge — so the parent offers a change notification, not an event.

**The watcher task is spawned in `run()`, not in `__init__`.** A running task
strongly references its frame, so spawning at construction makes every permit
immortal and re-creates the leak above via the event loop. Scoping it to
`run()` also means an executor built and never run costs nothing.

**`grant` takes the settle-down delay.** The suspender's `sleep` exists so a
signal that has just recovered cannot release the plan until it has held. Doing
that with a timer outside the permit means a release scheduled for one trip can
drop the reason raised by the next; the permit cancels a pending release when
the same key withholds again.

**Reasons are the state and mutate synchronously, from any thread.** A plan
built between a trip and the loop noticing it must not start unheld. Bringing
the loop-side view into step is the permit's own business, since it has the
loop — nothing outside can forget to do it.

**A trip while paused suspends when the plan resumes.** The permit never asks
what the plan is doing, so the reason simply stands and the supervisor acts on it
when the plan is running again. Main drops it: `state.is_running` is false while
paused, so no suspension is requested — and because the trip path is guarded by
`if self._ev is None`, no later trip can request one either, so the plan resumes
with the condition still bad and never suspends.

**`Msg('install_suspender')` is plan-local.** It withholds that plan's permit and
is unsubscribed when the plan ends. On main it is the same call as
`RE.install_suspender`, so a plan-installed suspender outlives the plan and
`suspend_wrapper` has to send `remove_suspender` by hand. The cost is
re-subscription churn for a plan that installs one in a loop, which is accepted.

**No checkpoint mid-plan aborts, and pushes nothing.** Same user-visible outcome
as main — print, `FailedPause`, abort — but it returns rather than falling through
to push a suspension onto a plan stack being torn down, which on main looks like
a missing early return.

**`get_futures` goes.** Its only caller was `RunEngine.__call__`, which meant a
headless caller driving a session directly got no pre-plan wait at all.

**`_run_permit` is left alone.** Pause is out of band — the run loop parks
between messages and resumes where it stopped — while suspension is in band and
rewinds. They also compose: you can be paused while suspended.

## Must not regress

- Two conditions tripping at once are one suspension carrying both
  justifications. On main each passes its own `_ev is None` test and pushes its
  own `_start_suspender`, so it rewinds twice and runs both pre-plans.
- A signal that recovers and trips again within `sleep` seconds stays withheld.
- No object holds a finished or never-run plan.

## Tests

Characterisation first, written against `main` and passing there, so the rewrite
is measured against pinned behaviour rather than memory:

1. Trips while a plan is running — one suspension: interruption recorded, motors
   stopped, rewind to the checkpoint, pre-plan, wait, post-plan, replay.
2. Releases while suspended — resumes `sleep` seconds later, replaying from the
   checkpoint.
3. Trips while idle — nothing suspends; the next plan waits before its first
   message.
4. Releases while idle — nothing is withheld; the next plan starts at once.

Then the new behaviour. These fail on `main` by design, and each one is a bug
this change exists to fix:

5. Two conditions tripping together — one suspension, one rewind, both
   justifications. Main rewinds twice and nests both pre-plans.
6. Recovers and trips again within `sleep` — stays withheld. Main's release is
   already scheduled and drops the newer reason.
7. Trips while paused — suspends on resume. Main never suspends again.
8. `Msg('install_suspender')` — unsubscribed when the plan ends. Main leaves it
   installed.
9. No checkpoint at plan start — waits, does not abort.
10. No checkpoint mid-plan — aborts with `FailedPause`.
11. Nothing retains a finished plan.

## Open

All remaining questions are in `questions-for-tom-caswell.md`: whether monitor
emission should trampoline onto the loop, what `RE.suspenders` and
`RE.ignore_callback_exceptions` should cover once one object becomes two, and
whose pre-plan runs when two conditions overlap.
