# Questions for Tom Caswell

Answers are quoted as blockquotes, in his words. A question moves from open to
closed when it has one, and closed entries record what was built from it.

# Open questions

## 1. Should monitor emission trampoline onto the loop?

There is an asymmetry on `main` that predates the session/executor split, and that
the split has so far deliberately not touched.

**Status callbacks are marshalled.** `RunEngine._add_status_to_group`'s
`done_callback` does nothing but
`self._loop.call_soon_threadsafe(self._status_object_completed, ...)`, so
everything that touches engine state runs on the loop thread.

**Monitor emission is not.** `RunBundler.monitor` hands its callback to
`obj.subscribe()` -- or, since #1923, to `obj.subscribe_reading()` -- and it emits
straight through to the dispatcher. For a synchronous ophyd v1 signal firing on
the device's own thread, that means **subscribers to a monitored signal are
invoked on the device's thread, not the loop's.**

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
demo branch with latency and plan-throughput measurements -- including the slow
subscriber case, which is the number that actually decides it -- if that would
help.

## 2. Should `RE.suspenders` report a plan's own suspenders?

> report the session and the executors

**Reopened.** We built the union you asked for, and are now going the other way.
Flagging it rather than quietly diverging.

The rewrite gives suspension a `Permit`: an object that is granted unless
something has a reason to withhold it. A session holds one, and each plan gets a
child of it, so a suspender installed on the session holds up every plan and one
installed from inside a plan, via `Msg('install_suspender')`, holds up only that
plan. A suspender's only collaborator becomes the permit it was installed on.

On `main` there is one place to install, so `RE.suspenders` lists everything.
With the split there are two.

The reason for changing our minds is symmetry with the two neighbours that have
the same shape. `RE.commands` reports the session's vocabulary, not the vocabulary
the running plan was built with. `RE.subscribe` reaches the session's dispatcher,
not the running plan's. Suspenders were the odd one out, reporting a union that
neither of the others does. Making all three mean "the durable ones, those that
outlive any plan" is one rule rather than three cases.

What it costs: a suspender a plan installed for itself is not enumerable from
outside that plan. It still holds that plan up, is still removed when the plan
ends, and `Msg('remove_suspender')` still reaches it from inside.

**What we need from you:** whether a plan's own suspenders being invisible at the
prompt is acceptable, given `RE.suspenders` is public API today.

# Closed questions

## Freezing at the launch of execute

> ?? command behaviour correct, freeze on launch of execute
> ?? md should be frozen on launch

Both confirmed by Tom Cobb.

Commands were already right: the registry is composed once in
`PlanExecutor.__init__` from a copy of the session's, `without_commands` is
consumed at construction, and no `Msg` reaches it, so `register_command` takes
effect for the next plan.

`md` was not. It was shared by reference with the session, deliberately, so its
contents could change under a running plan. It becomes a snapshot taken as the
executor is built. The session keeps whatever mapping it was given -- a
`PersistentDict`, say -- and only the contents are copied, so the persistent store
is never duplicated. `next_scan_id` deliberately reaches past the snapshot,
because the counter is durable and two concurrent plans must not be handed the
same id. This is a behaviour change against `main`, where `RE.md` is live, and
gets an `api_changes` entry.

## Holding the parent

> suspend is keep a reference to parent
> subscribe keeps reference to parent dispatcher: make dispatcher keep the ref

Permits already chain this way: a permit holds its parent and never a child, so a
finished plan's permit is not kept alive by the session's, and a suspender
installed on the session holds up every plan without being told about any of them.

Dispatchers now do the same. The session's dispatcher is the parent of each
plan's, and ordering documents -- the session's subscribers before the plan's --
is the dispatcher's own business. Previously the executor held a
`_parent_dispatcher` and its `emit` ordered the two by hand. As a result the
executor takes one `dispatcher`, the way it takes one `permit`.

## 3. Should `RE.ignore_callback_exceptions` reach a running plan's own subscribers?

> make this init state on RE

On `main` it is a property over `self.dispatcher.ignore_exceptions`, and there is
a single dispatcher, so plan-local subscribers -- from `RE(plan, subs=...)` and
`Msg('subscribe')` -- are covered by it too. The rewrite gives the session a
dispatcher and each plan its own, so a settable flag would have to be pushed to
each in turn.

Built as init state on the session, so there is nothing to push and a plan's
subscribers cannot behave differently from the rest.

## 4. When two conditions overlap, whose pre-plan should run?

> pre-plans all run in order they fired, at the time they fire, whether or not we are paused at the time. Write in docs that they should be idempotent to avoid open-close-open shutter
> post-plans run in the opposite order they were triggered in

Today each tripped suspender independently pushes its own suspension, so the plan
rewinds once per condition and the pre-plans run nested, in an order nobody
designed.

The rewrite collapses that: reasons accumulate on one permit and the plan is
suspended once, however many conditions stand, with every justification reported.
Per your answer, each condition's pre-plan still runs as it fires -- out of band,
since the plan is parked in the suspension's wait -- and the post-plans run in the
suspension's unwind, in reverse. The idempotence requirement is documented in
`api_changes.rst`.

This is what forced `Permit.reasons` to exist rather than a single merged
suspension: running each condition's own pre-plan needs them kept apart.
`Permit.wait_changed` came from the same place, so that a suspension already in
progress notices a condition joining it.

## 5. Does anyone use `suspender.install(RE)` or read `suspender.RE`?

> The first form possibly (do an isinstance then call RE.install_suspender and warn) the second form no

Built exactly as described: `install` takes a `Permit`, and an `isinstance` check
lets a `RunEngine` through with a `DeprecationWarning`, doing a durable install.
`suspender.RE` is gone.

Note for #1806, which rewrites the same method: it marshals the subscribe onto the
RunEngine's loop thread. With a permit there is no RunEngine, but a `Permit`
carries a loop, so `install(permit)` can marshal onto that instead.

## 6. Should `RunEngine.request_suspend` stay public?

> make private

It was a second route into suspension that bypassed the permit, so a suspension
raised through it would not merge with one raised by a suspender -- the double
rewind the permit exists to remove, through the side door.

`RunEngine.request_suspend` is deleted. The executor's is private: the permit
supervisor calls it to raise every suspension, so it is not only an entry point
for a `RunEngine`.
