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
