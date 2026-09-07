.. _headless:

Running plans without a RunEngine
=================================

A `RunEngine` is two things at once: the machinery that executes a plan, and the
machinery that drives that execution from a terminal on the main thread. Code
that is already inside an event loop -- a data acquisition service, a queue
consumer, a test -- wants the first without the second.

Those two halves are separate objects. A :class:`~bluesky.plan_executor.PlanSession`
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
    >>> from bluesky.plan_executor import PlanSession
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

Documents
---------

Subscribe on the session and you see every document from every plan it runs:

.. doctest::

    >>> def scan():
    ...     yield Msg("open_run")
    ...     yield Msg("close_run")
    ...
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

Suspending
----------

Suspension is a suspender's job, headless or not. Install one on the session and
it holds up every plan the session runs, for as long as its condition is bad::

    session.install_suspender(suspender)

Install one from inside a plan, with ``Msg('install_suspender')``, and it holds up
that plan alone and ends with it.

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
``session.suspensions``. It is empty when nothing is:

.. doctest::

    >>> async def main():
    ...     session = PlanSession()
    ...     return dict(session.suspensions)
    ...
    >>> asyncio.run(main())
    {}

Driving one
-----------

The lifecycle verbs are coroutines, because there is no main thread to block::

    await executor.pause()
    await executor.resume()
    await executor.stop()
    await executor.abort()
    await executor.halt()

``executor.state`` says where the plan is, and ``executor.hooks`` -- shared with
the session -- is where to attach a message hook or a progress display.
