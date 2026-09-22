.. _architecture:

Architecture: session, runner, suspension
===========================================

The objects behind the `RunEngine`, and which reaches which::

    RunEngine
      └── PlanSession          what outlives any one plan
            ├── Suspension         permission to run
            ├── Dispatcher     subscribers that outlive any plan
            ├── PlanHooks      where progress is watched from
            └── start(plan) -> PlanRunner
                                       ├── Suspension      child of the session's
                                       ├── Dispatcher  child of the session's
                                       └── PlanEnvironment (frozen)

The session builds one runner per plan and keeps no reference to it, so it can
run several plans at once. A `RunEngine` runs one at a time. See
:ref:`headless` for driving a session directly.

- **Things chain upward, never downward.** A plan's suspension and dispatcher
  hold the session's; nothing holds the plan below it.
- **A plan's environment does not change under it.** What a plan reads is
  settled when it is launched.

Permission to run
-----------------

.. autoclass:: bluesky.suspension.Suspension
    :members:
    :undoc-members:

A suspension is tripped while any reason stands, here or above it. Reasons are
kept apart, because each condition runs its own pre-plan.

.. autoclass:: bluesky.suspension.SuspensionReason
    :members:
    :undoc-members:

A suspender installed on the session trips the session's suspension and holds
up every plan. One installed by a plan, through ``Msg('install_suspender')``,
trips that plan's suspension only. A suspender knows only its suspension.

.. autoclass:: bluesky.suspenders.SuspenderBase
    :members: install, remove, tripped

.. _which-thread:

Which thread
------------

The `RunEngine` is the thread-safe object. The session, runner, suspensions and
dispatchers are used on the event loop only, and hold no locks.

Threads outside bluesky's control reach the loop through ``bluesky._loop``:

* ``run_coro_on_loop`` crosses and waits, for a user calling the `RunEngine`
  from their own thread;
* ``call_soon_or_now`` crosses without waiting, for ophyd completing a status,
  a signal calling a suspender back, or a monitor producing a document.

`test_every_hop_onto_the_loop_is_one_of_the_few_we_mean` pins these, plus the
loops ``bluesky.magics`` and ``bluesky.callbacks.zmq`` stop.

A trip is applied on the loop, so code that trips a signal and inspects the
engine in the next statement can see the old answer. The reasons are an
immutable mapping, swapped whole, so they can be read from any thread.

What outlives a plan
--------------------

.. autoclass:: bluesky.plan_session.PlanSession
    :members:
    :undoc-members:

.. autoclass:: bluesky.plan_runner.PlanHooks
    :members:
    :undoc-members:

The hooks are shared by reference with every runner, so setting ``RE.msg_hook``
mid-plan reaches the running plan.

What belongs to one plan
------------------------

.. autoclass:: bluesky.plan_runner.PlanEnvironment
    :members:
    :undoc-members:

A runner is given this rather than the session, so it has no route back to
one.

.. autoclass:: bluesky.plan_runner.PlanRunner
    :members: state, resumable, rewindable, suspenders, clear_suspenders,
              pause, resume, stop, deferred_pause_requested, done
    :special-members: __await__

Where documents go
------------------

.. autoclass:: bluesky.dispatcher.Dispatcher
    :members: process, subscribe, unsubscribe, ignore_exceptions

A plan's dispatcher passes each document to the session's subscribers before
its own. A plan's subscriptions end with its runner, and their tokens cannot
reach the session's. Documents are dispatched on the event loop.
