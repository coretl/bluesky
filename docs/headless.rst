.. _headless:

Running plans without a RunEngine
=================================

A `RunEngine` is two things at once: the machinery that executes a plan, and the
machinery that drives that execution from a terminal on the main thread. Code
that is already inside an event loop -- a data acquisition service, a queue
consumer, a test -- wants the first without the second.

Those two halves are separate objects. A :class:`~bluesky.plan_session.PlanSession`
holds everything that outlives any one plan: the metadata, the settings, the
document subscribers, the durable suspenders. A
:class:`~bluesky.plan_executor.PlanExecutor` executes exactly one plan and holds
everything belonging to that plan alone. One session builds many executors.

A `RunEngine` composes the two, and uses the same pair of calls you are about to.

Running one plan
----------------

.. doctest::

    >>> import asyncio
    >>> from bluesky import Msg
    >>> from bluesky.plan_session import PlanSession
    >>>
    >>> def plan():
    ...     yield Msg("null")
    ...     return "a value the plan returned"
    ...
    >>> async def main():
    ...     session = PlanSession()
    ...     executor = session.make_executor(plan())
    ...     return await executor.run()
    ...
    >>> asyncio.run(main())
    'a value the plan returned'

``make_executor`` builds an executor from the session's settings as they stand at
that moment, and hands it to you. The session does not keep a reference, which is
what lets one session run more than one plan at a time. ``run`` executes the plan
and returns what the plan returned; an executor runs once.

Because the plan is loaded as the executor is built, a malformed plan raises from
``make_executor`` rather than from ``run``.

How the plan ended
------------------

``run`` returns the plan's return value, which says nothing about how the plan
got there. The executor says that, and it is the same executor afterwards as
during -- it runs one plan and then holds the record of it:

.. doctest::

    >>> def scan():
    ...     yield Msg("open_run")
    ...     yield Msg("close_run")
    ...
    >>> async def main():
    ...     session = PlanSession()
    ...     executor = session.make_executor(scan())
    ...     await executor.run()
    ...     return executor.exit_status, executor.exit_reason, executor.interrupted
    ...
    >>> asyncio.run(main())
    ('success', '', False)

``exit_status`` is ``'success'``, ``'abort'`` or ``'fail'`` -- the same value
that goes on the RunStop document of every run the plan opened.
``exit_reason`` says why, when there is anything to say. ``interrupted`` is True
if the plan was paused, stopped, aborted or halted rather than reaching its own
end. ``run_start_uids`` lists every run it opened.

There is no result object to build. :class:`~bluesky.run_engine.RunEngineResult`
is what a `RunEngine` returns from ``__call__``, assembled out of exactly these
attributes; a caller that is not a `RunEngine` reads them directly.

When a plan fails
-----------------

A plan that raises raises out of ``run``. The executor still cleans up first --
stopping what it set, unstaging, closing open runs -- and still records how it
ended:

.. doctest::

    >>> def falls_over():
    ...     yield Msg("open_run")
    ...     raise RuntimeError("the detector fell over")
    ...
    >>> async def main():
    ...     session = PlanSession()
    ...     executor = session.make_executor(falls_over())
    ...     try:
    ...         await executor.run()
    ...     except RuntimeError:
    ...         pass
    ...     return executor.exit_status, executor.exit_reason
    ...
    >>> asyncio.run(main())
    ('fail', 'the detector fell over')

A failing status object raises :class:`~bluesky.utils.FailedStatus` the same
way. Nothing is swallowed and stored for you to find later, so a service can
let the exception travel: ``await executor.run()`` inside your own ``try`` is
the whole error-handling story.

Stopping a plan is not failing it. ``stop`` and its modes below end the plan
without raising, and set ``interrupted``.

Documents
---------

Subscribe on the session and you see every document from every plan it runs:

.. doctest::

    >>> async def main():
    ...     session = PlanSession()
    ...     names = []
    ...     session.dispatcher.subscribe(lambda name, doc: names.append(name))
    ...     await session.make_executor(scan()).run()
    ...     await session.make_executor(scan()).run()
    ...     return names
    ...
    >>> asyncio.run(main())
    ['start', 'stop', 'start', 'stop']

Subscribe for one plan and the subscription is discarded with its executor:

.. doctest::

    >>> async def main():
    ...     session = PlanSession()
    ...     durable, just_this_plan = [], []
    ...     session.dispatcher.subscribe(lambda name, doc: durable.append(name))
    ...     first = session.make_executor(scan(), subs={"start": lambda n, d: just_this_plan.append(n)})
    ...     await first.run()
    ...     await session.make_executor(scan()).run()
    ...     return durable, just_this_plan
    ...
    >>> asyncio.run(main())
    (['start', 'stop', 'start', 'stop'], ['start'])

A plan's dispatcher holds the session's as its parent, so a document reaches the
subscribers that outlive the plan before the ones that arrived with it.

Metadata
--------

The session holds the metadata that survives every plan, including the ``scan_id``
counter. Each plan is given a copy of it as the plan is launched, so writing to
``session.md`` takes effect for the next plan rather than the one already running:

.. doctest::

    >>> async def main():
    ...     session = PlanSession()
    ...     session.md["proposal"] = "p1234"
    ...     starts = []
    ...     session.dispatcher.subscribe(lambda name, doc: starts.append(doc), "start")
    ...     await session.make_executor(scan()).run()
    ...     return starts[0]["proposal"], starts[0]["scan_id"]
    ...
    >>> asyncio.run(main())
    ('p1234', 1)

``scan_id`` is the exception: it is allocated from the session as each run opens,
so two plans running at once are never handed the same one.

More than one plan at once
--------------------------

The session holds no executor, so nothing about it is single-plan. A
`RunEngine`'s one-plan-at-a-time rule is a `RunEngine`'s, because it has one
main thread to block; a service driving the loop itself does not:

.. doctest::

    >>> async def main():
    ...     session = PlanSession()
    ...     first = session.make_executor(scan())
    ...     second = session.make_executor(scan())
    ...     await asyncio.gather(first.run(), second.run())
    ...     return first.run_start_uids != second.run_start_uids, session.md["scan_id"]
    ...
    >>> asyncio.run(main())
    (True, 2)

What they share is the session, and only deliberately. Each plan's subscribers
see only its own documents while the session's see everything; each gets a
``scan_id`` of its own; and one plan failing does not disturb the other. What
they do share is the session's suspenders, which is the next section.

Suspending
----------

Holding a plan up is a suspender's job, headless or not. Install one on the
session and it holds up every plan the session runs, for as long as its
condition is bad::

    session.install_suspender(suspender)

Install one from inside a plan, with ``Msg('install_suspender')``, and it holds
up that plan alone and ends with it. The chain runs one way: a plan is held by
its own conditions and by the session's, and one plan's condition never reaches
another plan.

There is no separate way to raise a suspension by hand, and that is deliberate.
Holds have to compose: two conditions going bad at once are one suspension, and
the plan runs again when the last of them clears. A second route that set the
condition directly could not merge with the first, so whichever caller released
it would release the plan while the other still wanted it held. Writing the
condition as a suspender is what buys the composition.

So if a service needs to hold plans on something bluesky cannot see -- a health
endpoint, a queue message, a file that has to exist -- write a suspender for it.
:class:`~bluesky.suspenders.SuspenderBase` watches anything that can call it back
when a value changes; the shipped subclasses watch ophyd signals, but nothing in
the base class requires one.

A plan launched while a suspender is already tripped waits before its first
message, rather than starting and then suspending -- there is no checkpoint to
rewind to yet.

To ask whether anything is holding the session up, and what, read
``session.suspensions``. It is a mapping keyed by whoever raised each reason, and
empty when nothing is holding anything:

.. doctest::

    >>> async def main():
    ...     session = PlanSession()
    ...     return dict(session.suspensions)
    ...
    >>> asyncio.run(main())
    {}

Driving a running plan
----------------------

The lifecycle verbs are coroutines, because there is no main thread to block::

    await executor.pause()                                # at the next checkpoint
    await executor.pause(defer=False)                     # now
    await executor.resume()

Ending a plan early is one verb in three modes. ``success`` decides whether the
runs close as ``'success'`` or ``'abort'``; ``finalize`` decides whether the
plan may run its own cleanup on the way out::

    await executor.stop()                                 # RunEngine.stop
    await executor.stop(success=False)                    # RunEngine.abort
    await executor.stop(success=False, finalize=False)    # RunEngine.halt

There is no ``executor.abort`` or ``executor.halt``; those are the `RunEngine`
names for these three calls. ``executor.state`` says where the plan is.

Hooks
-----

Nothing here writes to standard output. A `RunEngine` prints what a plan has to
say because it is driving a terminal; a session says it through ``hooks``, which
it shares with every executor it builds, and which are unset by default -- so a
service that wants any of it must attach something:

``hooks.announce``
    Called with a line about what happened. A `RunEngine` wires this to
    ``print``.

``hooks.suspend``
    Called as a suspension begins, with the reasons standing, keyed by whoever
    raised them. The reasons rather than a sentence about them: a consumer that
    is handed prose can only print it, where one handed the mapping can count
    it, pick a condition out of it, or word it for its own users. Joining them
    into a sentence is what a `RunEngine` does with it.

``hooks.msg``
    Called with each ``Msg`` before it is processed. ``RE.msg_hook = print`` is
    the usual debugging idiom; this is what that sets.

``hooks.waiting``
    Called with the status objects a plan is waiting on, and with ``None`` once
    there is nothing left. Drives progress bars.

``hooks.state``
    Called ``f(new_state, old_state)`` on every state change.

``hooks.pause``
    Called with no arguments when an executor comes to rest paused. A
    `RunEngine` uses it to release the main thread; a headless caller has no
    thread to release and can leave it unset.

Nothing the executor says tells anyone which key to press: only a `RunEngine`
knows a keyboard is attached.
