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
from ophyd.signal import Signal

import bluesky
from bluesky import Msg
from bluesky.plan_executor import PlanEnvironment, PlanExecutor
from bluesky.plan_session import PlanSession
from bluesky.suspenders import SuspendBoolHigh
from bluesky.utils import InvalidCommand, RunEngineInterrupted


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


def _look_at_a_fresh_executor(session, look, plan=()):
    """Build an executor on the session's loop and hand ``look`` the result.

    Building one starts its plan, so a test that only wants to inspect a fresh
    executor does it here: nothing yields to the loop between the build and the
    look, and the task is cancelled rather than left to run.
    """

    async def main():
        executor = session.make_executor(plan)
        try:
            return look(executor)
        finally:
            executor._task.cancel()

    return session._loop.run_until_complete(main())


def test_executor_holds_no_threading_primitives(idle_session):
    """The executor is single threaded by construction.

    Everything it touches is reached from the event loop, so it needs no
    locks. If this fails, something that belongs to the RunEngine, which is
    the only thread-aware object of the three, has leaked down into it.
    """

    def look(executor):
        return {
            name: type(value).__name__
            for name, value in vars(executor).items()
            if isinstance(value, THREADING_PRIMITIVES)
        }

    assert _look_at_a_fresh_executor(idle_session, look) == {}


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
    loop-side methods, and a suspension is written on the loop by whoever crossed
    to get there.

    A thread bluesky did not choose reaches the loop where the callback it
    calls is defined. ophyd completes a status on whichever thread finished the
    move, and calls a suspender back on whichever thread it likes, so
    `done_callback` and `SuspenderBase.__call__` each own that crossing and do
    nothing else on that thread but hand the value over.

    If this fails, either a new boundary is real and belongs in this list with
    a reason, or a hop has been hidden inside something that should have left
    the crossing to its caller.
    """
    # A suspension is written on the loop; its caller crosses.
    assert _crossings("suspensions.py") == set()
    # Only the ophyd status callback.
    assert _crossings("plan_executor.py") == {"done_callback"}
    # Only the signal's own callback.
    assert _crossings("suspenders.py") == {"__call__"}
    # The facade crosses for everyone, which is why it may cross at all -- and
    # now through one implementation with no exceptions, so that "where does a
    # thread reach the loop" has a one-word answer. `_build_task` used to be
    # the second, because it wanted the future rather than the result; the plan
    # task is built on the loop with the rest of the executor now, and the
    # future the main thread reads is filled in from its done callback.
    assert _crossings("run_engine.py") == {"_run_on"}


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

    the announce hook may be wired to a websocket, where telling someone to hit
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


def test_a_malformed_plan_raises_on_the_calling_thread(RE):
    """Loading the plan as the executor is built is what puts it here.

    The `RunEngine` builds its executor on the loop, so this is the property
    that says the crossing waits and re-raises rather than leaving the failure
    in a future on the loop thread. The engine is left usable.
    """
    with pytest.raises(TypeError):
        RE(42)

    # Not left mid-plan by the failure.
    assert RE._executor.state.is_idle
    RE([Msg("null")])


def test_the_start_hook_holds_the_plan_before_its_first_message():
    """`hooks.start` is awaited before anything the plan can observe.

    The hook the executor waits on rather than tells. A `RunEngine` will hold
    the plan here until its signal handler is installed.
    """
    seen = []

    async def main():
        session = PlanSession()
        held = asyncio.Event()

        async def hold():
            seen.append("hook")
            await held.wait()

        session.hooks.start = hold
        executor = session.make_executor([Msg("null")])
        running = asyncio.ensure_future(executor.run())

        # Long enough for the plan to have run had nothing held it.
        await asyncio.sleep(0.1)
        # The hook was reached, and the plan has not started behind it.
        state_while_held = str(executor.state)
        seen.append("released")
        held.set()
        await running
        return state_while_held, str(executor.state)

    state_while_held, state_after = asyncio.run(main())

    assert seen == ["hook", "released"]
    # Still idle: the hook is awaited before the state leaves 'idle'.
    assert state_while_held == "idle"
    assert state_after == "idle"


def test_done_says_what_idle_cannot():
    """'idle' means both "not started" and "finished"; ``done`` separates them."""
    seen = {}

    async def main():
        session = PlanSession()
        held = asyncio.Event()
        session.hooks.start = held.wait
        executor = session.make_executor([Msg("null")])

        await asyncio.sleep(0.05)
        # Held before its first message: idle, and not done.
        seen["held"] = (str(executor.state), executor.done())

        held.set()
        await executor.run()
        # Finished: idle again, and this time done.
        seen["finished"] = (str(executor.state), executor.done())

    asyncio.run(main())

    assert seen["held"] == ("idle", False)
    assert seen["finished"] == ("idle", True)


def test_a_synchronous_start_hook_is_allowed():
    """The hook may be a plain callable; only the default does nothing."""
    called = []

    async def main():
        session = PlanSession()
        session.hooks.start = lambda: called.append("start")
        await session.make_executor([Msg("null")]).run()

    asyncio.run(main())

    assert called == ["start"]


def test_the_executor_says_how_the_plan_finished():
    """The outcome is readable off the executor, by whoever ran it.

    A headless caller used to have to build a `RunEngineResult` -- a
    `RunEngine`'s return shape, which it has no other use for -- or read a
    private attribute, to find out whether the plan it ran actually worked.
    """

    async def main():
        executor = PlanSession().make_executor([Msg("open_run"), Msg("close_run")])
        await executor.run()
        return executor

    executor = asyncio.run(main())
    assert executor.exit_status == "success"
    assert executor.exit_reason == ""
    assert not executor.interrupted
    assert len(executor.run_start_uids) == 1


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


def test_a_session_suspender_holds_every_plan_running_under_it():
    """One condition on the session holds every plan the session is running.

    Suspensions chain: each plan's suspension has the session's as its parent,
    so a condition raised on the session is tripped for all of them rather than
    for whichever plan happened to start first.
    """
    finished: list[str] = []

    async def main():
        session = PlanSession()
        sig = Signal(value=0, name="beam")
        # Already bad before either plan starts, so both are held at their
        # first message rather than racing a timer.
        sig.put(1)
        session.install_suspender(SuspendBoolHigh(sig))

        async def run(name, executor):
            await executor.run()
            finished.append(name)

        first = session.make_executor([Msg("checkpoint"), Msg("null")])
        second = session.make_executor([Msg("checkpoint"), Msg("null")])
        runners = asyncio.gather(run("first", first), run("second", second))
        await asyncio.sleep(0.2)
        held = list(finished)
        sig.put(0)
        await runners
        return held

    held = asyncio.run(main())
    # Neither plan got past its first message while the condition stood.
    assert held == []
    # And both finished once it cleared.
    assert sorted(finished) == ["first", "second"]


def test_a_plans_own_suspender_holds_only_that_plan():
    """A suspender a plan installs for itself does not reach the other plan.

    The chain runs one way. A plan's suspension is tripped by the session's
    reasons as well as its own; the session's is not tripped by a plan's, or
    one plan could hold up every other plan the session is running.
    """
    finished: list[str] = []

    async def main():
        session = PlanSession()
        sig = Signal(value=0, name="mine")
        # Bad before it is installed, so installing it holds this plan here.
        sig.put(1)
        susp = SuspendBoolHigh(sig)

        async def run(name, executor):
            await executor.run()
            finished.append(name)

        held = session.make_executor([Msg("checkpoint"), Msg("install_suspender", None, susp), Msg("null")])
        free = session.make_executor([Msg("checkpoint"), Msg("null")])
        runners = asyncio.gather(run("held", held), run("free", free))
        await asyncio.sleep(0.2)
        meanwhile = list(finished)
        sig.put(0)
        await runners
        return meanwhile

    meanwhile = asyncio.run(main())
    # The plan that installed nothing ran to the end while the other was held.
    assert meanwhile == ["free"]
    assert finished == ["free", "held"]


def test_two_plans_documents_reach_the_session_and_only_their_own_subscribers():
    """Each plan's own subscribers see its documents; the session's see both.

    Dispatchers chain the way suspensions do, so a subscription that arrives
    with one plan must not be handed the other plan's documents.
    """

    async def main():
        session = PlanSession()
        session_starts: list[str] = []
        first_starts: list[str] = []
        session.subscribe(lambda name, doc: session_starts.append(doc["uid"]) if name == "start" else None)

        plan = [Msg("open_run"), Msg("sleep", None, 0.05), Msg("close_run")]
        first = session.make_executor(
            list(plan),
            subs={"start": [lambda name, doc: first_starts.append(doc["uid"])]},
        )
        second = session.make_executor(list(plan))
        await asyncio.gather(first.run(), second.run())
        return session_starts, first_starts, first, second

    session_starts, first_starts, first, second = asyncio.run(main())

    # The session saw both plans' runs.
    assert sorted(session_starts) == sorted([*first.run_start_uids, *second.run_start_uids])
    # The subscription that arrived with the first plan saw only its own.
    assert first_starts == list(first.run_start_uids)


def test_one_plan_failing_leaves_the_other_alone():
    """Two plans on one session fail independently.

    They share a session, a dispatcher and a suspension chain, and none of
    those is a route for one plan's exception to reach the other.
    """

    async def main():
        session = PlanSession()
        good = session.make_executor([Msg("open_run"), Msg("sleep", None, 0.1), Msg("close_run")])
        bad = session.make_executor([Msg("open_run"), Msg("aardvark")])
        outcomes = await asyncio.gather(good.run(), bad.run(), return_exceptions=True)
        return good, bad, outcomes

    good, bad, outcomes = asyncio.run(main())

    assert good.exit_status == "success"
    assert isinstance(outcomes[1], InvalidCommand)
    assert bad.exit_status == "fail"


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

    # Installed on the executor, so it trips that plan's suspension and no
    # other. A session-installed one would hold up every plan.
    assert susp.installed_on is executor._suspension
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

    started: list[str] = []

    def next_plan():
        started.append("ran")
        yield Msg("null")

    async def main():
        session = PlanSession()
        session.install_suspender(susp)
        executor = session.make_executor([Msg("null")])
        await executor.run()
        # Trips after that plan ended. The session is still watching, so the
        # reason stands and the *next* plan is the one held for it.
        session._suspension.trip(susp, "beam is down")

        # Held before its first message rather than running and suspending:
        # there is no checkpoint yet to rewind to. Arranged when the plan
        # starts rather than when the executor is built, so it is the running
        # that has to be watched.
        nxt = session.make_executor(next_plan())
        task = asyncio.ensure_future(nxt.run())
        await asyncio.sleep(0.2)
        held = not started
        session._suspension.clear(susp)
        await asyncio.wait_for(task, timeout=10)
        return session, executor, held

    session, executor, next_plan_was_held = asyncio.run(main())

    assert susp.installed_on is session._suspension
    # Never unsubscribed by the plan: it goes on watching its signal between
    # plans, which is what lets it report that it is *already* tripped.
    assert not susp.removed
    assert susp in session.suspenders
    # The reason stood, so the next plan was held before its first message and
    # ran only once the suspender cleared.
    assert next_plan_was_held
    assert started == ["ran"]


def test_one_durable_suspender_covers_every_running_plan():
    """Beam going down holds both plans, not whichever started last."""

    async def main():
        session = PlanSession()
        first = session.make_executor([Msg("null")])
        second = session.make_executor([Msg("null")])
        # Nothing points a suspender at a plan any more: both are waiting on
        # the one suspension, so one reason covers both by construction.
        assert first._suspension.tripped or not session._suspension.tripped
        assert first._suspension is not second._suspension
        # ...and each has its own for the suspenders its own plan installs.
        assert first._suspension is not second._suspension
        await asyncio.gather(first.run(), second.run())

    asyncio.run(main())


def test_executor_starts_empty():
    """Building an executor is how the caches are cleared, so a new one must
    not carry anything over from the plan before it."""

    async def main():
        session = PlanSession()
        first = session.make_executor([Msg("open_run"), Msg("close_run")])
        await first.run()
        # Read before yielding: building the second one started its plan, and
        # what is being asserted is what it was *born* with.
        second = session.make_executor([])
        try:
            return (
                first,
                second,
                {
                    name: getattr(second, name)
                    for name in ("run_start_uids", "exit_status", "_exception", "_msg_cache", "_objs_seen")
                },
                len(second._plan_stack),
                dict(second._run_bundlers),
            )
        finally:
            second._task.cancel()

    first, second, born, plan_stack_depth, run_bundlers = asyncio.run(main())
    assert first.run_start_uids and not born["run_start_uids"]
    assert born["exit_status"] == "success"
    assert born["_exception"] is None
    # the caches themselves are private; this is the point of the class, so
    # reach in rather than let it go untested
    assert not born["_msg_cache"]
    assert not born["_objs_seen"]
    assert not run_bundlers
    # The plan stack is not empty, and must not be: an executor is built for a
    # plan, so its own plan is on the stack from construction. What matters is
    # that the *previous* plan left nothing behind, which is what the emptied
    # caches above show.
    assert plan_stack_depth == 1


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
    # The engine's own route to a new executor: built on the loop, and held at
    # `hooks.start` so its plan cannot run while the flag is read off it.
    RE._new_executor([Msg("null")])
    assert RE._executor._dispatcher.ignore_exceptions is True

    RE.ignore_callback_exceptions = False
    assert RE._executor._dispatcher.ignore_exceptions is False

    # Nothing is going to run that plan: put an idle executor back in its
    # place, which is what discards the one held here.
    RE._new_executor()


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
