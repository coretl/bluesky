"""Tests for the seam between PlanSession, PlanExecutor and RunEngine.

These guard the properties that make the executor usable without a
RunEngine: that it is pure asyncio, and that it can be driven directly.
"""

import asyncio
import dataclasses
import inspect
import pathlib
import threading

import pytest

import bluesky
from bluesky import Msg
from bluesky.permits import join_justifications
from bluesky.plan_executor import (
    PlanEnvironment,
    PlanExecutor,
    PlanSession,
)
from bluesky.utils import RunEngineInterrupted


class _RecordingSignal:
    """A Subscribable stand-in: enough for a suspender to be constructed."""

    name = "recording"

    def subscribe_reading(self, function):
        pass

    def clear_sub(self, function):
        pass


THREADING_PRIMITIVES = (
    threading.Event,
    threading.Lock().__class__,
    threading.RLock().__class__,
    threading.Condition,
    threading.Semaphore,
    threading.Barrier,
    threading.Thread,
)


@pytest.fixture
def idle_session():
    """A session on a loop of its own, for tests that never run a plan."""
    loop = asyncio.new_event_loop()
    try:
        yield PlanSession(loop=loop)
    finally:
        loop.close()


def test_the_old_import_location_still_works():
    """Both classes were defined in run_engine before they moved here, and it
    goes on re-exporting them for code written against that."""
    from bluesky import run_engine

    assert run_engine.PlanSession is PlanSession
    assert run_engine.PlanExecutor is PlanExecutor


def test_executor_holds_no_threading_primitives(idle_session):
    """The executor is single threaded by construction.

    Everything it touches is reached from the event loop, so it needs no
    locks. If this fails, something that belongs to the RunEngine, which is
    the only thread-aware object of the three, has leaked down into it.
    """
    executor = idle_session.make_executor([])

    offenders = {
        name: type(value).__name__
        for name, value in vars(executor).items()
        if isinstance(value, THREADING_PRIMITIVES)
    }
    assert offenders == {}


@pytest.mark.parametrize("cls", [PlanSession, PlanExecutor])
def test_source_takes_no_locks(cls):
    """Neither class ever blocks a thread, so neither may lock or join."""
    source = inspect.getsource(cls)
    for forbidden in ("threading.", "_state_lock", ".acquire(", ".join("):
        assert forbidden not in source


def _crossings(module_name):
    """The innermost function around every hop onto the loop in a module."""
    import ast

    source = pathlib.Path(bluesky.__file__).parent / module_name
    tree = ast.parse(source.read_text())
    crossing = {"call_soon_threadsafe", "run_coroutine_threadsafe"}
    found = set()

    def walk(node, enclosing):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                walk(child, child.name)
            else:
                if (
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Attribute)
                    and child.func.attr in crossing
                ):
                    found.add(enclosing)
                walk(child, enclosing)

    walk(tree, None)
    return found


def test_every_hop_onto_the_loop_is_one_of_the_few_we_mean():
    """Where a foreign thread reaches the loop, and why each one is allowed.

    Two rules, and every crossing here is one or the other.

    A caller's own thread reaches the loop only through the `RunEngine`. It is
    the thread-safe facade, so it may hop wherever it likes -- and nothing it
    calls hops for itself. `SuspenderBase.install` and `remove` are ordinary
    loop-side methods, and a permit is written on the loop by whoever crossed
    to get there.

    A thread bluesky did not choose reaches the loop where the callback it
    calls is defined. ophyd completes a status on whichever thread finished the
    move, calls a suspender back on whichever thread it likes, and calls a
    monitor callback on the device's own thread, so `done_callback`,
    `SuspenderBase.__call__` and `_queue_emit` each own that crossing and do
    nothing else on that thread but hand the value over.

    If this fails, either a new boundary is real and belongs in this list with
    a reason, or a hop has been hidden inside something that should have left
    the crossing to its caller.
    """
    # A permit is written on the loop; its caller crosses.
    assert _crossings("permits.py") == set()
    # The ophyd status callback, and a monitor callback queueing its document.
    assert _crossings("plan_executor.py") == {"done_callback", "_queue_emit"}
    # Only the signal's own callback.
    assert _crossings("suspenders.py") == {"__call__"}
    # The facade crosses for everyone, which is why it may cross at all -- but
    # through one implementation, so that "where does a thread reach the loop"
    # keeps a short answer. `_build_task` is the single exception, and is one
    # because it wants the future rather than the result: the wait that matters
    # for a running plan is `_resume_task` blocking on `_blocking_event`, which
    # Ctrl-C can interrupt where waiting on a future cannot.
    # One crossing, plus the plan task.
    assert _crossings("run_engine.py") == {"_run_on", "_build_task"}


def test_a_suspender_holds_no_threading_primitives():
    """A suspender's state is written on the loop and nowhere else.

    A reading is handed to the loop and decided there, so nothing guards it
    and no write has to be superseded on arrival.
    """
    from bluesky.suspenders import SuspendBoolHigh

    suspender = SuspendBoolHigh(_RecordingSignal())
    offenders = {
        name: type(value).__name__
        for name, value in vars(suspender).items()
        if isinstance(value, THREADING_PRIMITIVES)
    }
    assert offenders == {}


def test_the_executor_never_says_what_to_press():
    """It cannot know that a keyboard is attached.

    `announce_hook` may be wired to a websocket, where telling someone to hit
    Ctrl-C is wrong. Saying what to press belongs to the `RunEngine` and to
    `SigintHandler`, which exist only where a terminal does.
    """
    source = (pathlib.Path(bluesky.__file__).parent / "plan_executor.py").read_text()

    # Not even in a docstring; SIGINT is the accurate word here.
    assert "Ctrl" not in source


def test_session_holds_no_threading_primitives(idle_session):
    """The session is reachable from the main thread and from the loop, but
    everything that writes to it runs on the loop, so it needs no locks."""
    offenders = {
        name: type(value).__name__
        for name, value in vars(idle_session).items()
        if isinstance(value, THREADING_PRIMITIVES)
    }
    assert offenders == {}


def test_run_a_plan_without_a_run_engine():
    """A PlanExecutor executes a plan with no RunEngine in the process."""
    collected = []

    async def main():
        session = PlanSession(md={"beamline": "test"})
        session.dispatcher.subscribe(lambda name, doc: collected.append(name))
        executor = session.make_executor([Msg("open_run"), Msg("close_run")])
        plan_return = await executor.run()
        return executor, plan_return

    executor, plan_return = asyncio.run(main())

    assert collected == ["start", "stop"]
    assert len(executor.run_start_uids) == 1
    assert executor._exit_status == "success"
    assert executor.state == "idle"
    assert not executor.interrupted
    assert plan_return is None


def test_plan_return_value_without_a_run_engine():
    async def main():
        def plan():
            yield Msg("null")
            return 42

        executor = PlanSession().make_executor(plan())
        return await executor.run()

    assert asyncio.run(main()) == 42


def test_result_describes_the_finished_plan():
    async def main():
        executor = PlanSession().make_executor([Msg("open_run"), Msg("close_run")])
        plan_return = await executor.run()
        return executor.result(plan_return)

    result = asyncio.run(main())
    assert result.exit_status == "success"
    assert not result.interrupted
    assert result.reason == ""
    assert len(result.run_start_uids) == 1


def test_session_outlives_its_executors():
    """One session, many plans in turn. Metadata and subscriptions persist."""
    names = []

    async def main():
        session = PlanSession(md={"beamline": "test"})
        session.dispatcher.subscribe(lambda name, doc: names.append(name))
        uids = []
        for _ in range(3):
            executor = session.make_executor([Msg("open_run"), Msg("close_run")])
            await executor.run()
            uids.extend(executor.run_start_uids)
        return session, uids

    session, uids = asyncio.run(main())
    assert len(uids) == len(set(uids)) == 3
    assert names == ["start", "stop"] * 3
    # scan_id is persistent metadata, so it counts up across plans
    assert session.md["scan_id"] == 3


def test_two_plans_run_at_once_on_one_session():
    """A headless caller may run more than one plan against a session.

    The session holds no executor, so nothing about it is single-plan. The
    RunEngine's "one plan at a time" rule is the RunEngine's, because it has
    one main thread to block.
    """
    starts = []

    async def main():
        session = PlanSession(md={"beamline": "test"})
        session.dispatcher.subscribe(lambda name, doc: starts.append(doc) if name == "start" else None)
        plan = [Msg("open_run"), Msg("sleep", None, 0.05), Msg("close_run")]
        first = session.make_executor(list(plan))
        second = session.make_executor(list(plan))
        await asyncio.gather(first.run(), second.run())
        return session, first, second

    session, first, second = asyncio.run(main())

    assert first._exit_status == second._exit_status == "success"
    assert first.run_start_uids != second.run_start_uids
    # Each run was given a scan id of its own, rather than both reading back
    # whichever the other stored last.
    assert sorted(doc["scan_id"] for doc in starts) == [1, 2]
    assert session.md["scan_id"] == 2


def test_a_setting_reaches_the_next_plan_and_not_the_running_one():
    """Settings live on the session; each plan gets a frozen snapshot."""

    async def main():
        session = PlanSession()
        # The session keeps no environment of its own, so there is no second
        # copy of a setting to fall out of step with this one.
        assert not hasattr(session, "env") and not hasattr(session, "_env")

        already_built = session.make_executor([Msg("null")])
        session.strict_pre_declare = True
        built_after = session.make_executor([Msg("null")])

        # Frozen for its plan.
        assert already_built._env.strict_pre_declare is False
        # And live for the next.
        assert built_after._env.strict_pre_declare is True
        assert already_built._env is not built_after._env

        await asyncio.gather(already_built.run(), built_after.run())

    asyncio.run(main())


def test_metadata_is_snapshotted_and_the_session_keeps_its_own_store():
    """A plan reads the metadata as it stood when the plan was launched.

    The session's mapping is never replaced -- a ``PersistentDict`` stays the
    session's -- and never handed over either, so writing to it does not reach
    a plan already running.
    """

    async def main():
        session = PlanSession()
        store = {"from_the_store": True}
        session.md = store
        executor = session.make_executor([Msg("null")])

        assert executor._env.md == {"from_the_store": True}
        # A copy of the contents, not the store.
        assert executor._env.md is not store

        session.md["written_after_launch"] = True
        assert "written_after_launch" not in executor._env.md
        # And the session still holds what it was given.
        assert session.md is store

        await executor.run()

    asyncio.run(main())


def test_the_environment_describes_only_what_a_running_plan_reads():
    """Construction-only settings are arguments, not environment."""
    fields = {f.name for f in dataclasses.fields(PlanEnvironment)}
    # Both are consumed once, in __init__ -- preprocessors wrap the plan and
    # rewindable seeds a flag the plan then owns -- so an executor reading
    # either of them back from its environment would be reading a stale value.
    assert "preprocessors" not in fields
    assert "rewindable" not in fields


def test_a_plan_installs_a_suspender_for_itself_only():
    """Msg('install_suspender') belongs to the plan that sent it."""

    class _Susp:
        def __init__(self):
            self.installed_on = None
            self.removed = False

        def install(self, owner):
            self.installed_on = owner

        def remove(self):
            self.removed = True

    susp = _Susp()

    async def main():
        session = PlanSession()
        executor = session.make_executor([Msg("install_suspender", None, susp)])
        await executor.run()
        return session, executor

    session, executor = asyncio.run(main())

    # Installed on the executor, so it withholds that plan's permit and no
    # other. A session-installed one would hold up every plan.
    assert susp.installed_on is executor._permit
    assert susp.removed
    assert susp not in session.suspenders
    assert susp not in executor.suspenders


def test_a_durable_suspender_outlives_the_plan_it_held():
    """The session keeps the suspender, its subscription and its reason."""

    class _Susp:
        def __init__(self):
            self.installed_on = None
            self.removed = False

        def install(self, owner):
            self.installed_on = owner

        def remove(self):
            self.removed = True

    susp = _Susp()

    async def main():
        session = PlanSession()
        session.install_suspender(susp)
        executor = session.make_executor([Msg("null")])
        await executor.run()
        # Trips after that plan ended. The session is still watching, so the
        # reason stands and the *next* plan is the one held for it.
        session._permit.withhold(susp, "beam is down")
        return session, executor

    session, executor = asyncio.run(main())

    assert susp.installed_on is session._permit
    # Never unsubscribed by the plan: it goes on watching its signal between
    # plans, which is what lets it report that it is *already* tripped.
    assert not susp.removed
    assert susp in session.suspenders
    # And the reason stands, so the next plan waits for it before it starts.
    assert session.suspensions
    assert join_justifications(session.suspensions) == "beam is down"
    # Held by a prologue.
    assert session.make_executor([Msg("null")])._plan_stack


def test_one_durable_suspender_covers_every_running_plan():
    """Beam going down holds both plans, not whichever started last."""

    async def main():
        session = PlanSession()
        first = session.make_executor([Msg("null")])
        second = session.make_executor([Msg("null")])
        # Nothing points a suspender at a plan any more: both are waiting on
        # the one permit, so one reason covers both by construction.
        assert not first._permit.granted or session._permit.granted
        assert first._permit is not second._permit
        # ...and each has its own for the suspenders its own plan installs.
        assert first._permit is not second._permit
        await asyncio.gather(first.run(), second.run())

    asyncio.run(main())


def test_executor_starts_empty():
    """Building an executor is how the caches are cleared, so a new one must
    not carry anything over from the plan before it."""

    async def main():
        session = PlanSession()
        first = session.make_executor([Msg("open_run"), Msg("close_run")])
        await first.run()
        return first, session.make_executor([])

    first, second = asyncio.run(main())
    assert first.run_start_uids and not second.run_start_uids
    assert second._exit_status == "success"
    assert second._exception is None
    # the caches themselves are private; this is the point of the class, so
    # reach in rather than let it go untested
    assert not second._msg_cache
    assert not second._objs_seen
    assert not second._run_bundlers
    # The plan stack is not empty, and must not be: an executor is built for a
    # plan, so its own plan is on the stack from construction. What matters is
    # that the *previous* plan left nothing behind, which is what the emptied
    # caches above show.
    assert len(second._plan_stack) == 1


def test_run_engine_keeps_its_executor_after_the_plan(RE):
    """A finished plan can still be inspected through the RunEngine."""
    RE([Msg("open_run"), Msg("close_run")])
    assert len(RE._run_start_uids) == 1
    assert RE._exit_status == "success"
    # ...and the next plan gets a fresh executor
    previous = RE._executor
    RE([Msg("open_run"), Msg("close_run")])
    assert RE._executor is not previous
    assert len(RE._run_start_uids) == 1


def test_registered_commands_survive_a_new_executor(RE):
    """register_command is remembered by the session, so it outlives the
    executor that happened to be current when it was called."""
    seen = []

    async def custom(msg):
        seen.append(msg.command)

    RE.register_command("custom-command", custom)
    for _ in range(2):
        RE([Msg("custom-command")])
    assert seen == ["custom-command"] * 2

    RE.unregister_command("custom-command")
    with pytest.raises(KeyError):
        RE([Msg("custom-command")])


def test_request_pause_coro_survives_for_queueserver(RE):
    """bluesky-queueserver drives a non-blocking pause through this coroutine.

    Its worker cannot call the public ``request_pause``, which blocks and
    never returns if the loop is wedged, so it reaches for the private
    coroutine instead. There is no public equivalent yet, so this has to keep
    working.
    """

    def pause_from_another_thread():
        asyncio.run_coroutine_threadsafe(RE._request_pause_coro(False), loop=RE.loop).result()

    def plan():
        yield Msg("checkpoint")
        threading.Timer(0.1, pause_from_another_thread).start()
        yield Msg("sleep", None, 2)
        yield Msg("null")

    with pytest.raises(RunEngineInterrupted):
        RE(plan())
    assert RE.state == "paused"
    RE.stop()


def test_session_subscribers_see_a_document_before_the_plan_s(RE):
    """Ordering is the dispatcher chain's business, not the emitter's.

    A plan's dispatcher holds the session's as its parent, so a document
    reaches subscriptions that outlive the plan before the ones that arrived
    with it -- the order a single shared registry gave by construction.
    """
    seen = []
    RE.subscribe(lambda name, doc: seen.append(("session", name)), "start")

    RE(
        [Msg("open_run"), Msg("close_run")],
        {"start": lambda name, doc: seen.append(("plan", name))},
    )

    assert [who for who, _ in seen] == ["session", "plan"]


def test_ignore_callback_exceptions_is_read_live_by_a_plan(RE):
    """One setting, not one per dispatcher.

    A plan's dispatcher answers for its parent rather than copying the value
    when it is built, so setting the flag reaches the plan already running as
    well as every plan after it.
    """
    RE.ignore_callback_exceptions = True
    executor = RE._session.make_executor([Msg("null")])
    assert executor._dispatcher.ignore_exceptions is True

    RE.ignore_callback_exceptions = False
    assert executor._dispatcher.ignore_exceptions is False


def test_re_class_answers_for_whoever_is_driving(RE):
    """``Msg('RE_class')`` reports the class of the executor's ``identity``.

    A ``RunEngine`` names itself, so a plan asking what is running it gets the
    RunEngine rather than the executor that happens to be executing it. With
    nothing driving, an executor answers for itself, which is what a headless
    caller wants. The same value names the subject of a state change in the
    log.
    """
    seen = []

    def note():
        seen.append((yield Msg("RE_class")))

    RE(note())
    assert seen == [type(RE)]

    async def headless():
        seen.clear()
        await PlanSession().make_executor(note()).run()

    asyncio.run(headless())
    assert seen == [PlanExecutor]
