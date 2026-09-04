"""The four suspension sequences, and what permits change about them.

Tests 1-4 characterise behaviour that predates permits: they pass on ``main``
unchanged, so they measure the rewrite against pinned behaviour rather than
against memory. The rest are the bugs permits exist to fix, and fail on ``main``.
"""

import asyncio
import threading
import time as ttime

import pytest

from bluesky import Msg
from bluesky.permits import Permit
from bluesky.suspenders import SuspendBoolHigh
from bluesky.utils import FailedPause, RunEngineInterrupted

# A plan with a checkpoint to rewind to, and long enough to be interrupted.
SCAN = [Msg("checkpoint"), Msg("sleep", None, 0.2)]


def _at(delay, func, *args):
    threading.Timer(delay, func, args).start()


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
    _at(0.3, lambda: seen.append(RE.permit.suspension.justification))
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
    assert not RE.permit.granted, "the reason stands while paused"

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


# --------------------------------------------------------------------------
# The permit itself


def test_a_permit_is_withheld_for_every_thread_at_once():
    """`withhold` takes effect as it returns, not when the loop catches up."""
    permit = Permit("test", loop=asyncio.new_event_loop())
    permit.withhold("beam", "beam is down")
    assert not permit.granted
    assert permit.suspension.justification == "beam is down"
    permit.grant("beam")
    assert permit.granted
    assert permit.suspension is None


def test_a_child_permit_is_withheld_whenever_its_parent_is():
    """The chain, which is what makes durable and plan-local one mechanism."""
    loop = asyncio.new_event_loop()
    parent = Permit("session", loop=loop)
    child = Permit("plan", loop=loop, parent=parent)

    parent.withhold("beam", "beam is down")
    assert not child.granted, "held up by its parent"
    assert child.suspension.justification == "beam is down"

    child.withhold("shutter", "shutter is closed")
    parent.grant("beam")
    assert not child.granted, "still holding its own reason"
    assert parent.granted, "which is not the parent's business"

    child.grant("shutter")
    assert child.granted


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

    assert susp in RE.suspenders, "installed durably, as it used to be"
    sig.put(1)
    assert not RE.permit.granted, "and it withholds the engine's permit"
    sig.put(0)
    assert RE.permit.granted


def test_request_suspend_still_works_and_warns(RE, hw):
    """Deprecated, because suspension is raised by withholding the permit."""
    sig = hw.bool_sig
    sig.put(0)
    ev = asyncio.Event()

    def suspend_by_hand():
        with pytest.warns(DeprecationWarning, match="withholding"):
            RE.request_suspend(ev.wait)

    _at(0.1, suspend_by_hand)
    _at(0.5, RE.loop.call_soon_threadsafe, ev.set)
    start = ttime.time()
    RE(SCAN)
    delta = ttime.time() - start

    assert delta > 0.4, "the hand-rolled suspension still held the plan"
