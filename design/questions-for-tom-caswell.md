# Questions for Tom Caswell

## 1. Should monitor emission trampoline onto the loop?

There is an asymmetry on `main` that predates the session/executor split, and that
the split has so far deliberately not touched.

**Status callbacks are marshalled.** `RunEngine._add_status_to_group`'s
`done_callback` does nothing but
`self._loop.call_soon_threadsafe(self._status_object_completed, ...)`, so
everything that touches engine state runs on the loop thread.

**Monitor emission is not.** `RunBundler.monitor` hands `emit_event` to
`obj.subscribe()`, and it calls `emit_sync`, which is
`self.dispatcher.process(name, doc)` — straight through. For a synchronous
ophyd v1 signal firing on the device's own thread, that means **subscribers to a
monitored signal are invoked on the device's thread, not the loop's.**

Arguments both ways:

- **For marshalling.** A subscriber can be re-entered concurrently, from the
  device thread and the loop. `LiveTable`, `LivePlot` and the tiled writers are
  not written to be thread-safe. Marshalling gives one total order over the
  document stream instead of two interleaving producers.
- **Against.** The device thread currently absorbs slow callbacks. On the loop, a
  slow monitor callback stalls plan execution instead, and monitor documents
  queue behind a busy plan rather than going out promptly.

If it is done, it must not be unconditional: emitting from the loop thread has to
stay synchronous, or documents reorder relative to the plan.

**What we need from you:** whether this is worth changing at all, and if so
whether it goes in behind a flag defaulting to today's behaviour. We can build a
demo branch with latency and plan-throughput measurements — including the slow
subscriber case, which is the number that actually decides it — if that would
help.

## 2. Should `RE.suspenders` report a plan's own suspenders?

The rewrite gives suspension a `Permit`: an object that is granted unless
something has a reason to withhold it. A session holds one, and each plan gets a
child of it, so a suspender installed on the session holds up every plan and one
installed from inside a plan, via `Msg('install_suspender')`, holds up only that
plan. A suspender's only collaborator becomes the permit it was installed on.

On `main` there is one place to install, so `RE.suspenders` lists everything.
With the split there are two, and the interface no longer offers a public list of
a plan's own. So `RE.suspenders` would report the durable ones only, and a plan's
suspenders would not be enumerable from outside the plan that installed them.

That is defensible — they are the plan's business, and the plan installed them —
but it is a behaviour change, and the alternative is that `RE.suspenders` reports
the union of both.

**What we need from you:** which of those you would expect at the prompt, given
that `RE.suspenders` is public API today.

## 3. Should `RE.ignore_callback_exceptions` reach a running plan's own subscribers?

Same shape as question 2, from the same cause: one object becoming two.

On `main`, `RunEngine.ignore_callback_exceptions` is a property over
`self.dispatcher.ignore_exceptions`, and there is a single dispatcher. Plan-local
subscribers — those from `RE(plan, subs=...)` and `Msg('subscribe')` — land on
that same dispatcher, so the setting covers them too.

The rewrite gives the session a dispatcher for subscribers that outlive any plan,
and each plan's executor its own for subscribers that do not. Documents go to the
session's first, then the plan's. Plan-local subscriptions are then discarded with
the executor rather than unwound by hand at the end of a run.

Setting only the session's would leave a running plan's own subscribers on the
old value — a silent behaviour change. Options:

- a single public `ignore_exceptions` on the executor, so the `RunEngine` setter
  writes both;
- the plan's dispatcher takes the session's value when it is built and keeps it
  for the plan's duration, so setting the flag mid-plan does not reach the plan
  already running;
- move the setting somewhere shared and mutable that every plan reads live.

**What we need from you:** whether setting this mid-plan is expected to affect the
plan already running. If it is, the first option is the cheapest; if it was only
ever meant as setup, the second is simpler and we would document the change.

## 4. When two conditions overlap, whose pre-plan should run?

Today, each tripped suspender independently pushes its own suspension. In
`SuspenderBase.__call__` the trip path is guarded by `if self._ev is None`, so
the *existence* of that suspender's event is what marks "a suspension for this
condition is already outstanding" — but it is per suspender, so a second
suspender tripping while the first suspension is in flight passes its own guard
and calls `RE.request_suspend` again.

The result is a second `_start_suspender` on the plan stack: the plan rewinds
twice, and both pre-plans run, nested. If those pre-plans are not idempotent —
closing a shutter that is already closed, resetting a camera mid-reset — the
second rewind runs them in an order nobody designed.

The rewrite collapses that. Reasons accumulate on one permit, and a plan is
suspended once however many conditions are standing, with every justification
reported. That much seems clearly right. What it does not settle is the
pre-plans:

- **Extend the wait only.** Reasons that arrive after a suspension has begun
  hold the plan for longer and nothing else. Their pre-plans never run, on the
  grounds that the first suspender's pre-plan has already put the beamline in a
  safe state.
- **Run the newcomer's pre-plan too.** Right if pre-plans do genuinely different
  things per suspender, but it has to be injected into a wait the plan is
  already parked in.

Post-plans have the mirror-image question: run every reason's on the way out, or
only those whose pre-plan ran.

**What we need from you:** whether pre-plans are, in practice, per-suspender
work that must each happen, or a "make it safe" step where the first one is
enough. We have not found a case in the repo that distinguishes them.
