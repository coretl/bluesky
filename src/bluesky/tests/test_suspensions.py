"""The four suspension sequences, and what suspensions change about them.

Tests 1-4 characterise behaviour that predates suspensions: they pass on ``main``
unchanged, so they measure the rewrite against pinned behaviour rather than
against memory. The rest are the bugs suspensions exist to fix, and fail on ``main``.
"""

import asyncio
import concurrent.futures
import gc
import threading
import time as ttime
from collections.abc import Hashable, Mapping

import pytest
from ophyd.signal import Signal

from bluesky import Msg
from bluesky.suspenders import SuspendBoolHigh
from bluesky.suspensions import Suspension, SuspensionReason, join_justifications
from bluesky.tests import ophyd_async, requires_ophyd_async
from bluesky.utils import FailedPause, RunEngineInterrupted

from .utils import _at_message, force_suspension

if ophyd_async:
    from ophyd_async.core import soft_signal_rw

# A plan with a checkpoint to rewind to, and long enough to be interrupted.
SCAN = [Msg("checkpoint"), Msg("sleep", None, 0.2)]


def _at(delay, func, *args):
    threading.Timer(delay, func, args).start()


def _soft_signal(RE, name):
    """A soft signal, connected on the RunEngine's loop and reading false."""
    sig = soft_signal_rw(float, 0.0, name)
    asyncio.run_coroutine_threadsafe(sig.connect(), RE.loop).result()
    return sig


def _settle(RE):
    """Wait for the loop to apply what a signal callback just scheduled.

    A suspender trips on whatever thread its signal called back on and schedules
    the trip onto the loop rather than applying it there, so a test that
    puts a value and looks straight away is racing it. Everything reaches the
    loop in order, so one round trip behind the trip is enough.
    """
    done = concurrent.futures.Future()
    RE.loop.call_soon_threadsafe(lambda: done.set_result(None))
    done.result(timeout=10)


# --------------------------------------------------------------------------
# 1-4: the four sequences, as they behave on main


def test_trips_while_a_plan_is_running(RE, hw):
    """Sequence 1: rewind to the checkpoint, wait, then replay."""
    sig = hw.bool_sig
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig))
    commands = []
    RE.msg_hook = lambda msg: commands.append(msg.command)

    _at(0.1, sig.put, 1)
    _at(0.5, sig.put, 0)
    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    # Held until the signal recovered.
    assert delta > 0.4
    # Rewound to the checkpoint, so everything after it ran twice.
    assert commands.count("sleep") == 2
    assert commands.count("_start_suspender") == 1


def test_releases_while_a_plan_is_suspended(RE, hw):
    """Sequence 2: the settle-down sleep delays the release."""
    sig = hw.bool_sig
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig, sleep=0.3))

    _at(0.1, sig.put, 1)
    _at(0.4, sig.put, 0)
    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    # Released at 0.4, plus the 0.3 settle, plus the replayed 0.2 sleep.
    # Did not release before the signal had settled.
    assert delta > 0.4 + 0.3


def test_trips_while_no_plan_is_running(RE, hw):
    """Sequence 3: nothing suspends, but the next plan waits before it starts."""
    sig = hw.bool_sig
    sig.put(1)  # already bad before any plan exists
    RE.install_suspender(SuspendBoolHigh(sig))
    # A trip with no plan running suspends nothing.
    assert RE.state == "idle"

    commands = []
    RE.msg_hook = lambda msg: commands.append(msg.command)
    _at(0.5, sig.put, 0)
    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    # The plan waited for the signal before its first message.
    assert delta > 0.4
    # Held by a wait, not a suspension: there is no checkpoint yet to rewind
    # to, so nothing is replayed and no suspension is started.
    assert commands.count("sleep") == 1
    assert "_start_suspender" not in commands


def test_releases_while_no_plan_is_running(RE, hw):
    """Sequence 4: a condition that came and went holds nothing up."""
    sig = hw.bool_sig
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig))
    sig.put(1)
    sig.put(0)

    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    # Started without waiting.
    assert delta < 0.9


# --------------------------------------------------------------------------
# What suspensions change. These fail on main.


@requires_ophyd_async
def test_two_conditions_are_one_suspension(RE):
    """Both reasons are reported, and the plan rewinds once, not twice."""
    beam, shutter = _soft_signal(RE, "beam_sig"), _soft_signal(RE, "shutter_sig")
    RE.install_suspender(SuspendBoolHigh(beam, tripped_message="beam"))
    RE.install_suspender(SuspendBoolHigh(shutter, tripped_message="shutter"))

    commands, seen = [], []

    def both_bad():
        beam.set(1)
        shutter.set(1)

    def look_then_release():
        seen.append(join_justifications(RE._session.suspensions))
        beam.set(0)
        shutter.set(0)

    _at_message(RE, commands, sleep=both_bad, _start_suspender=look_then_release)
    RE(SCAN)

    assert len(seen) == 1
    # Both reasons reported.
    assert "beam" in seen[0] and "shutter" in seen[0]
    # One suspension. On main each tripped suspender pushes its own, so the
    # plan rewinds once per condition and runs both pre-plans nested.
    assert commands.count("_start_suspender") == 1


def test_a_retrip_within_the_settle_time_stays_tripped(RE, hw):
    """The release scheduled by one recovery must not drop a newer reason."""
    sig = hw.bool_sig
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig, sleep=0.4))

    _at(0.1, sig.put, 1)
    _at(0.3, sig.put, 0)  # schedules a release for 0.7
    _at(0.4, sig.put, 1)  # trips again before it comes due
    _at(0.9, sig.put, 0)
    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    # The stale release did not clear the newer reason.
    assert delta > 0.9 + 0.4


def test_trips_while_paused_suspends_on_resume(RE, hw):
    """A condition that goes bad while paused is not forgotten.

    On main the trip path tests ``state.is_running``, which is false while
    paused, so no suspension is ever requested -- and because that path is
    guarded by ``if self._ev is None``, no later trip can request one either.
    """
    sig = hw.bool_sig
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig))

    with pytest.raises(RunEngineInterrupted):
        RE([Msg("checkpoint"), Msg("pause"), Msg("sleep", None, 0.2)])
    assert RE.state == "paused"

    sig.put(1)
    _settle(RE)
    # The reason stands while paused.
    assert RE._session.suspensions

    _at(0.5, sig.put, 0)
    start = ttime.time()
    RE.resume()
    delta = ttime.time() - start
    # Resuming waited for the condition to clear.
    assert delta > 0.4


def test_no_checkpoint_mid_plan_aborts(RE, hw):
    """With nothing to rewind to, a suspension cannot happen; the plan aborts."""
    sig = hw.bool_sig
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig))

    _at(0.1, sig.put, 1)
    with pytest.raises(RunEngineInterrupted):
        RE([Msg("clear_checkpoint"), Msg("sleep", None, 0.5)])
    assert RE.state == "idle"
    # Aborted rather than suspended: there was nothing to rewind to.
    assert isinstance(RE._exception, FailedPause) or RE._exception is None


def test_no_checkpoint_abort_raises_nothing_into_the_loop(RE, hw):
    """Aborting arranges no suspension, so nothing is left to fail.

    The request runs in a fire-and-forget task, where an exception is reported
    only when the task is collected -- during some later, unrelated test.
    """
    reported = []
    RE.loop.call_soon_threadsafe(RE.loop.set_exception_handler, lambda loop, ctx: reported.append(ctx))

    sig = hw.bool_sig
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig))

    _at(0.1, sig.put, 1)
    with pytest.raises(RunEngineInterrupted):
        RE([Msg("clear_checkpoint"), Msg("sleep", None, 0.5)])
    assert RE.state == "idle"

    # The report is made from ``Task.__del__``, so collect before looking.
    gc.collect()
    # [ctx.get("message") for ctx in reported].
    assert not reported


def test_clear_suspenders_reaches_a_plans_own_from_the_prompt(RE, hw):
    """The escape hatch is reached from the prompt, never from the loop."""
    suspender = SuspendBoolHigh(hw.bool_sig)
    raised = []

    def clear_from_another_thread():
        try:
            RE.clear_suspenders()
        except BaseException as exc:  # noqa: BLE001
            raised.append(exc)

    def plan():
        yield Msg("install_suspender", None, suspender)
        yield Msg("checkpoint")
        yield Msg("sleep", None, 0.4)

    _at(0.2, clear_from_another_thread)
    RE(plan())

    # Repr(raised[0].
    assert not raised
    # And the plan's own suspender is gone.
    assert RE.suspenders == ()


def test_removing_a_suspender_settles_before_it_returns(RE, hw):
    """`remove` settles its grant before returning, as `install` does."""
    sig = hw.bool_sig
    sig.put(1)  # bad, and it has emitted, so the subscription reports it
    suspender = SuspendBoolHigh(sig)

    RE.install_suspender(suspender)
    suspension = RE._session._suspension
    # Installed on a bad signal, so it is holding.
    assert suspension.tripped

    RE.remove_suspender(suspender)
    # And it has let go by the time remove returns.
    assert not suspension.tripped


def test_installing_a_suspender_twice_is_an_error(RE, hw):
    """One suspender, one suspension: a second install would orphan the first."""
    suspender = SuspendBoolHigh(hw.bool_sig)
    RE.install_suspender(suspender)

    with pytest.raises(RuntimeError, match="already installed"):
        RE.install_suspender(suspender)

    RE.remove_suspender(suspender)
    RE.install_suspender(suspender)  # and removing frees it to be installed again


def test_a_pretripped_condition_runs_neither_of_its_plans(RE, hw):
    """A plan held at its first message runs no pre-plan, and so no post-plan.

    Tom Caswell's rule: a pre-plan reverses something a *plan* did, and no plan
    has run yet, so there is nothing to reverse -- something else may well be
    using the beamline. A post-plan undoes its pre-plan, so skipping one has to
    skip the other, or the plan would begin by opening a shutter it never
    closed.
    """
    sig = hw.bool_sig
    sig.put(1)  # already bad before anything is installed
    ran = []

    def note(tag):
        def plan():
            ran.append(tag)
            yield Msg("null")

        return plan

    RE.install_suspender(SuspendBoolHigh(sig, pre_plan=note("pre"), post_plan=note("post")))
    commands = []
    RE.msg_hook = lambda msg: commands.append(msg.command)

    _at(0.4, sig.put, 0)
    start = ttime.time()
    RE(SCAN)

    # The plan really was held.
    assert ttime.time() - start > 0.3
    # Neither plan ran.
    assert ran == []
    # Held, rather than suspended.
    assert commands.count("_start_suspender") == 0


def test_a_condition_joining_a_suspension_runs_its_pre_plan(RE):
    """A condition joining an open suspension gets its pre-plan run too.

    The plan is held inside the suspension, not parked away from it: it wakes
    on the join, runs the second condition's pre-plan in band on its own stack,
    and goes back to holding.
    """
    first = Signal(value=0, name="first")
    second = Signal(value=0, name="second")
    first.put(0)
    second.put(0)
    finished = []

    def pre(name):
        def plan():
            yield Msg("null")
            finished.append(name)

        return plan

    RE.install_suspender(SuspendBoolHigh(first, pre_plan=pre("first")))
    RE.install_suspender(SuspendBoolHigh(second, pre_plan=pre("second")))

    _at(0.1, first.put, 1)
    _at(0.3, second.put, 1)
    _at(0.6, lambda: (first.put(0), second.put(0)))
    RE([Msg("checkpoint")] + [Msg("sleep", None, 0.2)] * 5)

    # Both pre-plans ran to completion.
    assert finished == ["first", "second"]


def test_a_joining_pre_plan_that_raises_reaches_the_plan(RE):
    """A joiner's pre-plan is the plan's work, so its exception is the plan's.

    It used to be run off the plan stack by the supervisor task, which nobody
    awaits: the exception killed that task silently and resurfaced later as
    "Task exception was never retrieved" against whatever test happened to be
    running when the loop got round to reporting it.
    """
    first = Signal(value=0, name="first")
    second = Signal(value=0, name="second")

    def fine():
        yield Msg("null")

    def raises():
        yield Msg("null")
        raise RuntimeError("joiner pre-plan")

    RE.install_suspender(SuspendBoolHigh(first, pre_plan=fine))
    RE.install_suspender(SuspendBoolHigh(second, pre_plan=raises))

    _at(0.1, first.put, 1)
    _at(0.3, second.put, 1)
    # A safety net, so a failure to propagate shows up as a failed assertion
    # rather than as a hung suite.
    _at(1.5, lambda: (first.put(0), second.put(0)))

    with pytest.raises(RuntimeError, match="joiner pre-plan"):
        RE([Msg("checkpoint")] + [Msg("sleep", None, 0.2)] * 10)

    RE.clear_suspenders()


def test_a_suspension_arriving_after_the_plan_ends_does_nothing(RE):
    """The supervisor's task can outlive the plan that created it.

    A suspension raised through the test back door can reach an executor that
    has already gone idle. Neither transition it would attempt is legal from
    'idle'.
    """
    RE([Msg("null")])
    executor = RE._executor
    assert executor.state.is_idle

    force_suspension(RE, justification="too late").result(timeout=10)

    # And it left the state alone.
    assert executor.state.is_idle


def test_a_suspension_reaches_both_hooks(RE):
    """The event goes to the suspend hook; everything else to the announce hook.

    The suspend hook is handed the reasons, not prose about them. A headless
    consumer needs to know *what* tripped, which a joined string has already
    thrown away; joining is `RunEngine`'s business, because printing is.
    """
    said: list[str] = []
    suspensions: list[Mapping[Hashable, SuspensionReason]] = []
    RE._session.hooks.announce = said.append
    RE._session.hooks.suspend = suspensions.append

    sig = Signal(value=0, name="s")
    sig.put(0)
    susp = SuspendBoolHigh(sig)
    RE.install_suspender(susp)

    _at(0.1, sig.put, 1)
    _at(0.5, sig.put, 0)
    RE([Msg("checkpoint")] + [Msg("sleep", None, 0.2)] * 4)

    # The suspension was reported as an event.
    assert suspensions
    # Keyed by whoever raised it, so a consumer can tell which condition it was
    # rather than having to parse a sentence.
    (reasons,) = suspensions
    assert list(reasons) == [susp]
    # And the justification is still reachable, by joining it here.
    assert join_justifications(reasons) == "Signal s is high"
    # And nothing announced a key to press.
    assert "Ctrl" not in "".join(said)


# --------------------------------------------------------------------------
# The suspension itself


def test_a_permit_is_read_from_any_thread():
    """Reports cross freely. Only the writes are pinned to the loop.

    The reasons are swapped rather than mutated, so a reader off the loop sees
    one snapshot or the next -- never a mapping being merged as it is unpacked.
    """
    loop = asyncio.new_event_loop()
    suspension = Suspension("test", loop=loop)

    async def trip():
        suspension.trip("beam", "beam is down")

    loop.run_until_complete(trip())

    seen = {}

    def read():
        seen["tripped"] = suspension.tripped
        seen["why"] = join_justifications(suspension.reasons)

    reader = threading.Thread(target=read)
    reader.start()
    reader.join()
    loop.close()

    assert seen == {"tripped": True, "why": "beam is down"}


def test_a_child_suspension_is_tripped_whenever_its_parent_is():
    """The chain, which is what makes durable and plan-local one mechanism."""

    async def check():
        loop = asyncio.get_running_loop()
        parent = Suspension("session", loop=loop)
        child = Suspension("plan", loop=loop, parent=parent)

        parent.trip("beam", "beam is down")
        # Held up by its parent.
        assert child.tripped
        assert join_justifications(child.reasons) == "beam is down"

        child.trip("shutter", "shutter is closed")
        parent.clear("beam")
        # Still holding its own reason.
        assert child.tripped
        # Which is not the parent's business.
        assert not parent.tripped

        child.clear("shutter")
        assert not child.tripped

    asyncio.run(check())


def test_pre_plans_run_in_fire_order_and_post_plans_in_reverse(RE, hw):
    """A condition joining a suspension still runs its own pre-plan, when it fires.

    Pre-plans go in the order their conditions fired; post-plans in the reverse
    of it, so the last thing done is the first undone. A condition that joins
    an episode already in progress does not start a second rewind.
    """
    from ophyd import Signal

    beam, shutter = hw.bool_sig, Signal(name="shutter_sig", value=0)
    beam.put(0)
    order = []

    def note(tag):
        def plan():
            order.append(tag)
            yield Msg("null")

        return plan

    RE.install_suspender(
        SuspendBoolHigh(beam, pre_plan=note("beam-pre"), post_plan=note("beam-post"), tripped_message="beam")
    )
    RE.install_suspender(
        SuspendBoolHigh(
            shutter, pre_plan=note("shutter-pre"), post_plan=note("shutter-post"), tripped_message="shutter"
        )
    )
    commands = []
    RE.msg_hook = lambda msg: commands.append(msg.command)

    _at(0.1, beam.put, 1)
    _at(0.3, shutter.put, 1)
    _at(0.6, beam.put, 0)
    _at(0.6, shutter.put, 0)
    RE(SCAN)

    assert order == ["beam-pre", "shutter-pre", "shutter-post", "beam-post"]
    # Still one rewind, however many conditions joined it.
    assert commands.count("_start_suspender") == 1


@requires_ophyd_async
def test_two_conditions_tripping_in_one_turn_each_run_their_plans(RE):
    """Both conditions are in the opening snapshot, rather than one joining.

    Two signals going bad in the same turn of the loop -- one interlock dropping
    two readings -- have both trips applied before the supervisor is
    scheduled again, so neither arrives through the joining path. Each must
    still run its own pre-plan, and the post-plans still unwind in reverse.

    Both sets are made from one call on the loop, so the two tasks are queued
    before the supervisor is woken by the first of them: "the same turn" is
    arranged rather than hoped for.
    """
    beam, shutter = _soft_signal(RE, "beam_sig"), _soft_signal(RE, "shutter_sig")
    order = []

    def note(tag):
        def plan():
            order.append(tag)
            yield Msg("null")

        return plan

    RE.install_suspender(SuspendBoolHigh(beam, pre_plan=note("beam-pre"), post_plan=note("beam-post")))
    RE.install_suspender(SuspendBoolHigh(shutter, pre_plan=note("shutter-pre"), post_plan=note("shutter-post")))
    commands = []

    def both_bad():
        beam.set(1)
        shutter.set(1)

    def both_good():
        beam.set(0)
        shutter.set(0)

    _at_message(RE, commands, sleep=both_bad, _start_suspender=both_good)
    RE([Msg("checkpoint")] + [Msg("sleep", None, 0.2)] * 5)

    assert order == ["beam-pre", "shutter-pre", "shutter-post", "beam-post"]
    # Still one rewind, as when they arrive one after the other.
    assert commands.count("_start_suspender") == 1


def test_a_trip_between_building_the_plan_and_running_it_still_holds():
    """The window between `make_executor` and the plan's first message.

    Whether the suspension is tripped is read when the plan starts, not when the
    executor is built. It used to be read at both, and the two could disagree:
    a condition going bad in between left the plan with nothing holding it and
    a supervisor that believed it was already being held, so the plan ran to
    completion through a tripped suspender.

    A headless caller can hold an executor for as long as it likes before
    awaiting it, so the window is as wide as it chooses.
    """
    from bluesky.plan_session import PlanSession

    steps = []

    def plan():
        yield Msg("checkpoint")
        for _ in range(3):
            steps.append("step")
            yield Msg("sleep", None, 0.05)

    async def main():
        session = PlanSession()
        executor = session.make_executor(plan())
        # Nothing had tripped when this was built.
        assert not executor._suspension.tripped

        session._suspension.trip("beam", "beam is down")
        task = asyncio.ensure_future(executor)
        await asyncio.sleep(0.3)
        held = list(steps)

        session._suspension.clear("beam")
        await asyncio.wait_for(task, timeout=10)
        return held

    ran_while_tripped = asyncio.run(main())

    assert ran_while_tripped == []
    assert steps == ["step"] * 3


def test_a_trip_just_after_the_plan_starts_still_suspends(RE, hw):
    """The window between the plan starting and the supervisor's first turn.

    A condition already bad when the plan starts is held by a wait before the
    first message, not a suspension, because there is no checkpoint yet. That
    must not swallow a condition going bad immediately afterwards, which is a
    real trip and has to rewind.
    """
    sig = hw.bool_sig
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig))
    commands = []

    def trip_as_the_plan_starts(new_state, old_state):
        if (old_state, new_state) == ("idle", "running"):
            # The plan has started, so __call__ has read the suspension already,
            # and the supervisor has been created but has not had a turn.
            sig.put(1)

    RE.state_hook = trip_as_the_plan_starts
    RE.msg_hook = lambda msg: commands.append(msg.command)
    _at(0.5, sig.put, 0)
    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    # The trip was not swallowed.
    assert delta > 0.4
    # And it suspended rather than merely waiting.
    assert commands.count("_start_suspender") == 1


def test_installing_a_suspender_on_the_run_engine_still_works(RE, hw):
    """`install(RE)` was the old spelling. It warns, and installs durably."""
    sig = hw.bool_sig
    sig.put(0)
    susp = SuspendBoolHigh(sig)

    with pytest.warns(DeprecationWarning, match="takes the suspension"):
        susp.install(RE)

    # The deprecated call installs on the engine.
    assert susp in RE.suspenders
    sig.put(1)
    _settle(RE)
    # And it holds up the engine.
    assert RE._session.suspensions
    sig.put(0)
    _settle(RE)
    assert not RE._session.suspensions
