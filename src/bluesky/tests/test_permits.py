"""The four suspension sequences, and what permits change about them.

Tests 1-4 characterise behaviour that predates permits: they pass on ``main``
unchanged, so they measure the rewrite against pinned behaviour rather than
against memory. The rest are the bugs permits exist to fix, and fail on ``main``.
"""

import asyncio
import concurrent.futures
import gc
import threading
import time as ttime

import pytest
from ophyd.signal import Signal

from bluesky import Msg
from bluesky.permits import Permit, join_justifications
from bluesky.suspenders import SuspendBoolHigh
from bluesky.utils import FailedPause, RunEngineInterrupted

# A plan with a checkpoint to rewind to, and long enough to be interrupted.
SCAN = [Msg("checkpoint"), Msg("sleep", None, 0.2)]


def _at(delay, func, *args):
    threading.Timer(delay, func, args).start()


def _settle(RE):
    """Wait for the loop to apply what a signal callback just scheduled.

    A suspender trips on whatever thread its signal called back on and schedules
    the withhold onto the loop rather than applying it there, so a test that
    puts a value and looks straight away is racing it. Everything reaches the
    loop in order, so one round trip behind the withhold is enough.
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

    assert delta > 0.4, "held until the signal recovered"
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
    assert delta > 0.4 + 0.3, "did not release before the signal had settled"


def test_trips_while_no_plan_is_running(RE, hw):
    """Sequence 3: nothing suspends, but the next plan waits before it starts."""
    sig = hw.bool_sig
    sig.put(1)  # already bad before any plan exists
    RE.install_suspender(SuspendBoolHigh(sig))
    assert RE.state == "idle", "a trip with no plan running suspends nothing"

    commands = []
    RE.msg_hook = lambda msg: commands.append(msg.command)
    _at(0.5, sig.put, 0)
    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    assert delta > 0.4, "the plan waited for the signal before its first message"
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

    assert delta < 0.9, "started without waiting"


# --------------------------------------------------------------------------
# What permits change. These fail on main.


def test_two_conditions_are_one_suspension(RE, hw):
    """Both reasons are reported, and the plan rewinds once, not twice."""
    from ophyd import Signal

    beam, shutter = hw.bool_sig, Signal(name="shutter_sig", value=0)
    beam.put(0)
    RE.install_suspender(SuspendBoolHigh(beam, tripped_message="beam"))
    RE.install_suspender(SuspendBoolHigh(shutter, tripped_message="shutter"))
    commands = []
    RE.msg_hook = lambda msg: commands.append(msg.command)

    seen = []
    _at(0.1, beam.put, 1)
    _at(0.15, shutter.put, 1)
    _at(0.3, lambda: seen.append(join_justifications(RE._session.suspensions)))
    _at(0.5, beam.put, 0)
    _at(0.5, shutter.put, 0)
    RE(SCAN)

    assert len(seen) == 1
    assert "beam" in seen[0] and "shutter" in seen[0], "both reasons reported"
    # One suspension. On main each tripped suspender pushes its own, so the
    # plan rewinds once per condition and runs both pre-plans nested.
    assert commands.count("_start_suspender") == 1


def test_a_retrip_within_the_settle_time_stays_withheld(RE, hw):
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

    assert delta > 0.9 + 0.4, "the stale release did not clear the newer reason"


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
    assert RE._session.suspensions, "the reason stands while paused"

    _at(0.5, sig.put, 0)
    start = ttime.time()
    RE.resume()
    delta = ttime.time() - start
    assert delta > 0.4, "resuming waited for the condition to clear"


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
    assert not reported, [ctx.get("message") for ctx in reported]


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

    assert not raised, repr(raised[0])
    assert RE.suspenders == (), "and the plan's own suspender is gone"


def test_removing_a_suspender_settles_before_it_returns(RE, hw):
    """`remove` settles its grant before returning, as `install` does."""
    sig = hw.bool_sig
    sig.put(1)  # bad, and it has emitted, so the subscription reports it
    suspender = SuspendBoolHigh(sig)

    RE.install_suspender(suspender)
    permit = RE._session._permit
    assert not permit.granted, "installed on a bad signal, so it is holding"

    RE.remove_suspender(suspender)
    assert permit.granted, "and it has let go by the time remove returns"


def test_installing_a_suspender_twice_is_an_error(RE, hw):
    """One suspender, one permit: a second install would orphan the first."""
    suspender = SuspendBoolHigh(hw.bool_sig)
    RE.install_suspender(suspender)

    with pytest.raises(RuntimeError, match="already installed"):
        RE.install_suspender(suspender)

    RE.remove_suspender(suspender)
    RE.install_suspender(suspender)  # and removing frees it to be installed again


def test_a_condition_joining_a_suspension_runs_its_pre_plan(RE):
    """A pre-plan runs off the plan stack, so it needs the executor's commands.

    The plan is parked in the suspension's ``wait_for`` and will not reach
    anything pushed onto its stack, so the second condition's pre-plan is
    worked off out of band.
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

    assert finished == ["first", "second"], "both pre-plans ran to completion"


def test_a_suspension_reaches_both_hooks(RE):
    """The event goes to `suspend_hook`; everything else to `announce_hook`."""
    said: list[str] = []
    suspensions: list[str] = []
    RE._session.hooks.announce_hook = said.append
    RE._session.hooks.suspend_hook = suspensions.append

    sig = Signal(value=0, name="s")
    sig.put(0)
    RE.install_suspender(SuspendBoolHigh(sig))

    _at(0.1, sig.put, 1)
    _at(0.5, sig.put, 0)
    RE([Msg("checkpoint")] + [Msg("sleep", None, 0.2)] * 4)

    assert suspensions, "the suspension was reported as an event"
    assert "Ctrl" not in "".join(said), "and nothing announced a key to press"


# --------------------------------------------------------------------------
# The permit itself


def test_a_permit_is_read_from_any_thread():
    """Reports cross freely. Only the writes are pinned to the loop.

    The reasons are swapped rather than mutated, so a reader off the loop sees
    one snapshot or the next -- never a mapping being merged as it is unpacked.
    """
    loop = asyncio.new_event_loop()
    permit = Permit("test", loop=loop)

    async def withhold():
        permit.withhold("beam", "beam is down")

    loop.run_until_complete(withhold())

    seen = {}

    def read():
        seen["granted"] = permit.granted
        seen["why"] = join_justifications(permit.withheld_by)

    reader = threading.Thread(target=read)
    reader.start()
    reader.join()
    loop.close()

    assert seen == {"granted": False, "why": "beam is down"}


def test_a_child_permit_is_withheld_whenever_its_parent_is():
    """The chain, which is what makes durable and plan-local one mechanism."""

    async def check():
        loop = asyncio.get_running_loop()
        parent = Permit("session", loop=loop)
        child = Permit("plan", loop=loop, parent=parent)

        parent.withhold("beam", "beam is down")
        assert not child.granted, "held up by its parent"
        assert join_justifications(child.withheld_by) == "beam is down"

        child.withhold("shutter", "shutter is closed")
        parent.grant("beam")
        assert not child.granted, "still holding its own reason"
        assert parent.granted, "which is not the parent's business"

        child.grant("shutter")
        assert child.granted

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


def test_two_conditions_tripping_in_one_turn_each_run_their_plans(RE, hw):
    """Both conditions are in the opening snapshot, rather than one joining.

    Two signals going bad in the same turn of the loop -- one interlock dropping
    two readings -- have both withholds applied before the supervisor is
    scheduled again, so neither arrives through the joining path. Each must
    still run its own pre-plan, and the post-plans still unwind in reverse.
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

    RE.install_suspender(SuspendBoolHigh(beam, pre_plan=note("beam-pre"), post_plan=note("beam-post")))
    RE.install_suspender(SuspendBoolHigh(shutter, pre_plan=note("shutter-pre"), post_plan=note("shutter-post")))
    commands = []
    RE.msg_hook = lambda msg: commands.append(msg.command)

    def both_bad():
        beam.put(1)
        shutter.put(1)

    _at(0.1, both_bad)
    _at(0.6, lambda: (beam.put(0), shutter.put(0)))
    RE([Msg("checkpoint")] + [Msg("sleep", None, 0.2)] * 5)

    assert order == ["beam-pre", "shutter-pre", "shutter-post", "beam-post"]
    # Still one rewind, as when they arrive one after the other.
    assert commands.count("_start_suspender") == 1


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
            # The plan has started, so __call__ has read the permit already,
            # and the supervisor has been created but has not had a turn.
            sig.put(1)

    RE.state_hook = trip_as_the_plan_starts
    RE.msg_hook = lambda msg: commands.append(msg.command)
    _at(0.5, sig.put, 0)
    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    assert delta > 0.4, "the trip was not swallowed"
    assert commands.count("_start_suspender") == 1, "and it suspended rather than merely waiting"


def test_installing_a_suspender_on_the_run_engine_still_works(RE, hw):
    """`install(RE)` was the old spelling. It warns, and installs durably."""
    sig = hw.bool_sig
    sig.put(0)
    susp = SuspendBoolHigh(sig)

    with pytest.warns(DeprecationWarning, match="takes the permit"):
        susp.install(RE)

    assert susp in RE.suspenders, "the deprecated call installs on the engine"
    sig.put(1)
    _settle(RE)
    assert RE._session.suspensions, "and it holds up the engine"
    sig.put(0)
    _settle(RE)
    assert not RE._session.suspensions
