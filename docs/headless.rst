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
:class:`~bluesky.plan_runner.PlanRunner` executes exactly one plan and holds
everything belonging to that plan alone. One session builds many runners.

A `RunEngine` composes the two, and reaches them the same way you are about to.

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
    ...     runner = session.start(plan())
    ...     return await runner
    ...
    >>> asyncio.run(main())
    'a value the plan returned'

``start`` builds a runner from the session's settings as they stand at
that moment, sets the plan going, and hands it to you. The session does not keep
a reference, which is what lets one session run more than one plan at a time.
Awaiting the runner waits for the plan and gives back what it returned; a
runner is built for one plan and runs it once.

Because the plan is loaded as the runner is built, a malformed plan raises from
``start`` rather than out of the await.

How the plan ended
------------------

Awaiting a runner gives the plan's return value, which says nothing about
how the plan got there. The runner says that, and it is the same runner afterwards as
during -- it runs one plan and then holds the record of it:

.. doctest::

    >>> def scan():
    ...     yield Msg("open_run")
    ...     yield Msg("close_run")
    ...
    >>> async def main():
    ...     session = PlanSession()
    ...     runner = session.start(scan())
    ...     await runner
    ...     return runner.exit_status, runner.exit_reason, runner.interrupted
    ...
    >>> asyncio.run(main())
    ('success', '', False)

``exit_status`` is ``'success'``, ``'abort'`` or ``'fail'`` -- the same value
that goes on the RunStop document of every run the plan opened.
``exit_reason`` says why, when there is anything to say. ``interrupted`` is True
if the plan was paused, stopped, aborted or halted rather than reaching its own
end. ``run_start_uids`` lists every run it opened.

``done()`` says whether the plan has finished at all, which ``state`` cannot:
a runner reports ``'idle'`` both before its plan reaches the first message
and after the plan has ended.

There is no result object to build. :class:`~bluesky.run_engine.RunEngineResult`
is what a `RunEngine` returns from ``__call__``, assembled out of exactly these
attributes; a caller that is not a `RunEngine` reads them directly.

When a plan fails
-----------------

A plan that raises raises out of the await. The runner still cleans up first --
stopping what it set, unstaging, closing open runs -- and still records how it
ended:

.. doctest::

    >>> def falls_over():
    ...     yield Msg("open_run")
    ...     raise RuntimeError("the detector fell over")
    ...
    >>> async def main():
    ...     session = PlanSession()
    ...     runner = session.start(falls_over())
    ...     try:
    ...         await runner
    ...     except RuntimeError:
    ...         pass
    ...     return runner.exit_status, runner.exit_reason
    ...
    >>> asyncio.run(main())
    ('fail', 'the detector fell over')

A failing status object raises :class:`~bluesky.utils.FailedStatus` the same
way. Nothing is swallowed and stored for you to find later, so a service can
let the exception travel: ``await runner`` inside your own ``try`` is
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
    ...     session.subscribe(lambda name, doc: names.append(name))
    ...     await session.start(scan())
    ...     await session.start(scan())
    ...     return names
    ...
    >>> asyncio.run(main())
    ['start', 'stop', 'start', 'stop']

Subscribe for one plan and the subscription is discarded with its runner:

.. doctest::

    >>> async def main():
    ...     session = PlanSession()
    ...     durable, just_this_plan = [], []
    ...     session.subscribe(lambda name, doc: durable.append(name))
    ...     first = session.start(scan(), subs={"start": lambda n, d: just_this_plan.append(n)})
    ...     await first
    ...     await session.start(scan())
    ...     return durable, just_this_plan
    ...
    >>> asyncio.run(main())
    (['start', 'stop', 'start', 'stop'], ['start'])

A plan's subscribers sit under the session's, so a document reaches the
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
    ...     session.subscribe(lambda name, doc: starts.append(doc), "start")
    ...     await session.start(scan())
    ...     return starts[0]["proposal"], starts[0]["scan_id"]
    ...
    >>> asyncio.run(main())
    ('p1234', 1)

``scan_id`` is the exception: it is allocated from the session as each run opens,
so two plans running at once are never handed the same one.

More than one plan at once
--------------------------

The session holds no runner, so nothing about it is single-plan. A
`RunEngine`'s one-plan-at-a-time rule is a `RunEngine`'s, because it has one
main thread to block; a service driving the loop itself does not:

.. doctest::

    >>> async def main():
    ...     session = PlanSession()
    ...     first = session.start(scan())
    ...     second = session.start(scan())
    ...     await asyncio.gather(first, second)
    ...     return first.run_start_uids != second.run_start_uids, session.md["scan_id"]
    ...
    >>> asyncio.run(main())
    (True, 2)

What they share is the session, and only deliberately. Each plan's subscribers
see only its own documents while the session's see everything; each gets a
``scan_id`` of its own; and one plan failing does not disturb the other. What
they do share is the session's suspenders, which is the next section.

.. warning::

    Two plans running at once must not touch the same hardware. Nothing stops
    them: a runner knows what its own plan has staged, set and triggered, and
    knows nothing at all about anybody else's. Two plans moving one motor, or
    staging one detector, will interleave their messages and leave the device
    in a state neither plan asked for.

    Keeping them apart is the caller's job, and there is no mechanism here to
    help -- so if two plans might reach the same device, run them one after
    another. This restriction belongs to the plans, not to the runners.

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

    await runner.pause()                                # at the next checkpoint
    await runner.pause(defer=False)                     # now
    await runner.resume()

Ending a plan early is one verb in three modes. ``success`` decides whether the
runs close as ``'success'`` or ``'abort'``; ``finalize`` decides whether the
plan may run its own cleanup on the way out::

    await runner.stop()                                 # RunEngine.stop
    await runner.stop(success=False)                    # RunEngine.abort
    await runner.stop(success=False, finalize=False)    # RunEngine.halt

There is no ``runner.abort`` or ``runner.halt``; those are the `RunEngine`
names for these three calls. ``runner.state`` says where the plan is.

Hooks
-----

Nothing here writes to standard output. A `RunEngine` prints what a plan has to
say because it is driving a terminal; a session says it through ``hooks``, which
it shares with every runner it builds, and which are unset by default -- so a
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

``hooks.start``
    Awaited before the plan's first message, and may be a coroutine. A
    `RunEngine` uses it to hold the plan until its signal handler is installed;
    a headless caller has none to install and can leave it unset. It is the one
    hook the runner waits on rather than tells, so a hook that returns late
    holds the plan late.

``hooks.pause``
    Called with no arguments when a runner comes to rest paused. A
    `RunEngine` uses it to release the main thread; a headless caller has no
    thread to release and can leave it unset.

Nothing the runner says tells anyone which key to press: only a `RunEngine`
knows a keyboard is attached.
