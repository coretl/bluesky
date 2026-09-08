.. _architecture:

Architecture: session, executor, permit
=======================================

This page describes what the objects behind the `RunEngine` are, and which of
them reaches which. Every class listed here is generated from the code, so the
descriptions cannot drift from what is actually there.

The shape of it
---------------

A `RunEngine` used to be one object holding two unrelated kinds of state: the
things that outlive any plan -- your metadata, your subscribers, your suspenders
-- and the things belonging to the plan in flight -- its state machine, its
caches, its checkpoint. Those are now two objects, and the `RunEngine` composes
them::

    RunEngine
      └── PlanSession          what outlives any one plan
            ├── Permit         permission to run
            ├── Dispatcher     subscribers that outlive any plan
            ├── PlanHooks      where progress is watched from
            └── make_executor(plan) -> PlanExecutor
                                       ├── Permit      child of the session's
                                       ├── Dispatcher  child of the session's
                                       └── PlanEnvironment (frozen)

The session builds one executor per plan and keeps no reference to it, which is
what lets code that is already inside an event loop run more than one plan at
once. A `RunEngine` drives exactly one, and enforces that itself, because a
terminal has one main thread to block. See :ref:`headless` for driving a session
directly.

Two rules explain most of the design.

**Things chain upward, never downward.** A plan's permit holds its parent; a
plan's dispatcher holds its parent. Nothing holds a reference to the plan below
it, so a finished plan's state is not kept alive by the session, and a suspender
installed on the session holds up every plan without being told about any of
them.

**A plan's environment does not change under it.** Everything the running plan
reads is settled when it is launched.

Permission to run
-----------------

.. autoclass:: bluesky.permits.Permit
    :members:
    :undoc-members:

A permit is granted exactly when no reason stands, here or anywhere above it. The
reasons are the state, and they are kept apart rather than merged, because each
condition runs its own pre-plan as it fires.

``withhold`` and ``grant`` run on the event loop and raise anywhere else, while
``granted`` and the standing reasons can be read from any thread. See
:ref:`which-thread` for why the writes are pinned and the reads are not.

.. _which-thread:

Which thread
------------

The `RunEngine` is the thread-safe object. Everything it drives -- the session,
the executor, the permit, the dispatchers -- runs on the event loop, and calling
into them from another thread is a bug rather than a slow path. That is why
`test_executor_holds_no_threading_primitives` and `test_source_takes_no_locks`
pass: single-threaded by construction needs no locks.

Three boundaries are real, because something outside bluesky picks the thread:

* a user calls the `RunEngine` from their own thread;
* ophyd completes a status on whichever thread finished the move, and the
  executor attached that callback;
* a signal calls a suspender back on whichever thread it likes.

At each one, the object that owns the boundary owns the hop, and does it with
``call_soon_threadsafe``. A permit does not, which is the difference from how
this started: a permit used to hop internally, so every one of its methods hid
a thread boundary and the one object that knew which thread it was standing on
-- the suspender -- was not the one deciding. ``__on_loop`` waits for the loop
and must never be called holding the suspender's lock; ``__tell_loop`` does not
wait and is what a trip uses.

`test_every_hop_onto_the_loop_is_one_of_the_few_we_mean` pins that list, so a
fourth crossing cannot appear without saying so.

What the writes being loop-only costs: a trip is applied where the permit's
state lives rather than where the signal fired, so code that trips a signal and
inspects the engine in the next statement can see the old answer. Starting a
plan after a trip is unaffected, because everything reaches the loop in order.

What it buys is that the reads need nothing. The reasons are an immutable
mapping, replaced rather than mutated, so a reader on any thread sees one
snapshot or the next. Those readers -- ``RunEngine.suspenders``, the standing
suspensions, a message printed for a human -- report rather than decide, and the
only reader that decides is the supervisor, which is on the loop.

`CallbackRegistry` is the exception, and not by choice. It drops a subscriber
from a weakref destroy callback, which runs on whichever thread collected it,
and that cannot be scheduled onto the loop. So it takes a lock, and snapshots
its subscribers before calling them rather than holding that lock across
arbitrary user code.

.. autoclass:: bluesky.permits.Suspension
    :members:
    :undoc-members:

The chain is what makes durable and plan-local suspension one mechanism. A
suspender installed on the session withholds the session's permit and holds up
every plan run under it. One installed by a plan, through
``Msg('install_suspender')``, withholds that plan's permit and holds up that plan
alone. A suspender's only collaborator is the permit it was installed on: it never
learns what, or whether, anything is running.

.. autoclass:: bluesky.suspenders.SuspenderBase
    :members: install, remove, tripped

What outlives a plan
--------------------

.. autoclass:: bluesky.plan_executor.PlanSession
    :members:
    :undoc-members:

A constructor argument means a setting nothing changes once the session exists:
``md``, ``loop`` and ``log``, which are consumed while it is built, and
``run_bundler_cls`` and ``identity``, which whoever constructs it decides once.
Everything else is a plain attribute, because changing it later is part of the
interface -- a `RunEngine` has a property for each of them. ``make_executor``
reads those into a frozen
:class:`~bluesky.plan_executor.PlanEnvironment` for each plan, so changing one
takes effect for the next plan and never the one already running, and the
session never holds a second copy of a setting to keep in step. Nothing appears
in both halves except ``md``, which has to, because the versions are stamped
into it as the session is built and ``RE.md`` can still be reassigned.

.. autoclass:: bluesky.plan_executor.PlanHooks
    :members:
    :undoc-members:

The hooks are the exception to the rule above: one mutable record, shared by
reference with every executor, so that setting ``RE.msg_hook`` mid-plan reaches
the plan already running. They are debugging and display attachments rather than
anything a plan's meaning depends on.

What belongs to one plan
------------------------

.. autoclass:: bluesky.plan_executor.PlanEnvironment
    :members:
    :undoc-members:

Frozen on purpose. An executor is given one of these instead of the session that
built it, so it can be constructed and tested without a session, and so that it
has no route back to one.

Settings that are consumed once at construction are arguments to the executor
rather than part of the environment -- ``preprocessors``, which wrap the plan
once, and the ``rewindable`` default, which seeds a flag the plan then owns.
Reading either back from the environment would be reading a stale value.

.. autoclass:: bluesky.plan_executor.PlanExecutor
    :members: run, state, resumable, rewindable_flag, suspenders, clear_suspenders,
              pause, resume, stop, abort, halt, emit, deferred_pause_requested

``run_start_uids`` lists every run the plan has opened, and ``interrupted`` says
whether it was stopped before it finished.

An executor runs one plan, once. ``identity`` is what its state changes are
logged as having happened to, and what ``Msg('RE_class')`` reports the class of:
a `RunEngine` names itself, because that is what a user recognises in a log,
while a headless executor answers for itself.

Where documents go
------------------

.. autoclass:: bluesky.plan_executor.Dispatcher
    :members: process, subscribe, unsubscribe, ignore_exceptions

Ordering is the dispatcher's own business rather than the emitter's: a plan's
dispatcher gives a document to its parent's subscribers before its own, which is
the order a single shared registry gave by construction. Making the lifetime
structural means tearing down a plan's subscriptions is nothing more than
dropping its executor, and it gives plan tokens a namespace of their own, so a
plan cannot unsubscribe a session callback by guessing an integer.

Emission is synchronous and happens on whichever thread produced the document.
For a plan's own documents that is the event loop; for a monitored synchronous
ophyd signal it is the device's thread.
