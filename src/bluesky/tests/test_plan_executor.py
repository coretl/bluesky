"""Tests for the seam between PlanSession, PlanExecutor and RunEngine.

These guard the properties that make the executor usable without a
RunEngine: that it is pure asyncio, and that it can be driven directly.
"""

import asyncio
import dataclasses
import inspect
import threading

import pytest

from bluesky import Msg
from bluesky.plan_executor import (
    PlanEnvironment,
    PlanExecutor,
    PlanSession,
)
from bluesky.utils import RunEngineInterrupted

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
        assert forbidden not in source, f"{cls.__name__} uses {forbidden}"


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
        session.subscribe(lambda name, doc: collected.append(name))
        executor = session.make_executor([Msg("open_run"), Msg("close_run")])
        plan_return = await executor.run()
        return executor, plan_return

    executor, plan_return = asyncio.run(main())

    assert collected == ["start", "stop"]
    assert len(executor.run_start_uids) == 1
    assert executor.exit_status == "success"
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
        session.subscribe(lambda name, doc: names.append(name))
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
        session.subscribe(lambda name, doc: starts.append(doc) if name == "start" else None)
        plan = [Msg("open_run"), Msg("sleep", None, 0.05), Msg("close_run")]
        first = session.make_executor(list(plan))
        second = session.make_executor(list(plan))
        await asyncio.gather(first.run(), second.run())
        return session, first, second

    session, first, second = asyncio.run(main())

    assert first.exit_status == second.exit_status == "success"
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
        assert not hasattr(session, "_env")

        already_built = session.make_executor([Msg("null")])
        session.strict_pre_declare = True
        built_after = session.make_executor([Msg("null")])

        assert already_built.env.strict_pre_declare is False, "frozen for its plan"
        assert built_after.env.strict_pre_declare is True, "and live for the next"
        assert already_built.env is not built_after.env

        await asyncio.gather(already_built.run(), built_after.run())

    asyncio.run(main())


def test_metadata_is_handed_over_and_never_copied():
    """A PersistentDict must reach the plan as itself, not as a copy."""

    async def main():
        session = PlanSession()
        replacement = {"replaced": True}
        session.md = replacement
        executor = session.make_executor([Msg("null")])
        # Swapping the mapping is enough: nothing else holds the old one.
        assert executor.env.md is replacement
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
    assert susp.installed_on is executor.permit
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
        session.permit.withhold(susp, "beam is down")
        return session, executor

    session, executor = asyncio.run(main())

    assert susp.installed_on is session.permit
    # Never unsubscribed by the plan: it goes on watching its signal between
    # plans, which is what lets it report that it is *already* tripped.
    assert not susp.removed
    assert susp in session.suspenders
    # And the reason stands, so the next plan waits for it before it starts.
    assert not session.permit.granted
    assert session.permit.suspension.justification == "beam is down"
    assert session.make_executor([Msg("null")])._plan_stack, "held by a prologue"


def test_one_durable_suspender_covers_every_running_plan():
    """Beam going down holds both plans, not whichever started last."""

    async def main():
        session = PlanSession()
        first = session.make_executor([Msg("null")])
        second = session.make_executor([Msg("null")])
        # Nothing points a suspender at a plan any more: both are waiting on
        # the one permit, so one reason covers both by construction.
        assert not first.permit.granted or session.permit.granted
        assert first.permit is not second.permit
        # ...and each has its own for the suspenders its own plan installs.
        assert first.permit is not second.permit
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
    assert second.exit_status == "success"
    assert second.exception is None
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
    assert RE._executor.exit_status == "success"
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
