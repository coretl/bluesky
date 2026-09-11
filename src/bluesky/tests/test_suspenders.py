import asyncio
import threading
import time
import time as ttime
from functools import partial

import pytest
from ophyd.signal import Signal

from bluesky import Msg
from bluesky.preprocessors import suspend_wrapper
from bluesky.run_engine import RunEngineInterrupted
from bluesky.suspenders import (
    SuspendBoolHigh,
    SuspendBoolLow,
    SuspendCeil,
    SuspendFloor,
    SuspendInBand,
    SuspendOutBand,
    SuspendWhenChanged,
    SuspendWhenOutsideBand,
)
from bluesky.tests import ophyd_async, requires_ophyd_async
from bluesky.tests.utils import MsgCollector

from .utils import _fabricate_asycio_event, suspend_until

if ophyd_async:
    from ophyd_async.core import soft_signal_rw

parametrize_suspenders = pytest.mark.parametrize(
    "klass,sc_args,start_val,fail_val,resume_val,wait_time",
    [
        (SuspendBoolHigh, (), 0, 1, 0, 0.2),
        (SuspendBoolLow, (), 1, 0, 1, 0.2),
        (SuspendFloor, (0.5,), 1, 0, 1, 0.2),
        (SuspendCeil, (0.5,), 0, 1, 0, 0.2),
        (SuspendWhenOutsideBand, (0.5, 1.5), 1, 0, 1, 0.2),
        ((SuspendInBand, True), (0.5, 1.5), 1, 0, 1, 0.2),  # renamed to WhenOutsideBand
        ((SuspendOutBand, True), (0.5, 1.5), 0, 1, 0, 0.2),
    ],
)  # deprecated


def _check_suspender(klass, sc_args, sig, putter, start_val, fail_val, resume_val, wait_time, RE):
    try:
        klass, deprecated = klass
    except TypeError:
        deprecated = False
    if deprecated:
        with pytest.warns(UserWarning):
            my_suspender = klass(sig, *sc_args, sleep=wait_time)
    else:
        my_suspender = klass(sig, *sc_args, sleep=wait_time)
    RE.install_suspender(my_suspender)

    # make sure we start at good value!
    putter(start_val)
    # dumb scan
    scan = [Msg("checkpoint"), Msg("sleep", None, 0.2)]
    RE(scan)
    # paranoid
    assert RE.state == "idle"

    start = ttime.time()
    # queue up fail and resume conditions
    threading.Timer(0.1, putter, (fail_val,)).start()
    threading.Timer(0.5, putter, (resume_val,)).start()
    # start the scan
    RE(scan)
    stop = ttime.time()
    # assert we waited at least 2 seconds + the settle time
    delta = stop - start
    print(delta)
    # The suspension time is actually 0.5 - 0.1 = 0.4 seconds as timers run in parallel
    assert delta > 0.4 + wait_time + 0.2


@parametrize_suspenders
def test_suspender(klass, sc_args, start_val, fail_val, resume_val, wait_time, RE, hw):
    sig = hw.bool_sig

    def putter(val):
        sig.put(val)

    _check_suspender(klass, sc_args, sig, putter, start_val, fail_val, resume_val, wait_time, RE)


class _RecordingSignal:
    """A minimal Subscribable that records where it was called from."""

    name = "sig"

    def __init__(self):
        self.threads = {}

    def subscribe_reading(self, function):
        self.threads["subscribe_reading"] = threading.get_ident()
        function({self.name: {"value": 0, "timestamp": 0}})

    def clear_sub(self, function):
        self.threads["clear_sub"] = threading.get_ident()


@pytest.mark.parametrize("via_plan", [False, True])
def test_subscribes_on_run_engine_thread(RE, via_plan):
    "Subscriptions belong to the event loop that made them, e.g. CA monitors"
    sig = _RecordingSignal()
    susp = SuspendBoolHigh(sig)

    if via_plan:
        # install/remove are reached from the event loop thread this way
        RE([Msg("install_suspender", None, susp), Msg("remove_suspender", None, susp)])
    else:
        # Reached from this thread, so the RunEngine does the crossing.
        RE.install_suspender(susp)
        RE.remove_suspender(susp)

    loop_thread = _loop_thread_ident(RE)
    assert sig.threads == {"subscribe_reading": loop_thread, "clear_sub": loop_thread}


def test_remove_without_install_does_not_need_a_loop():
    sig = _RecordingSignal()

    SuspendBoolHigh(sig).remove()

    assert sig.threads == {}


def _loop_thread_ident(RE):
    """The ident of the thread the RunEngine's event loop runs in."""

    async def ident():
        return threading.get_ident()

    return asyncio.run_coroutine_threadsafe(ident(), RE.loop).result()


def _connected_soft_signal(RE, initial_value):
    """Make a soft signal connected on the RunEngine's event loop."""
    sig = soft_signal_rw(float, initial_value, "sig")
    asyncio.run_coroutine_threadsafe(sig.connect(), RE.loop).result()
    return sig


def _set_on_loop(RE, sig, value):
    """Set a signal from another thread.

    ``set`` makes an ``AsyncStatus`` as soon as it is called, so it has to be
    called on the event loop rather than merely awaited there.
    """

    async def set_it():
        await sig.set(value)

    asyncio.run_coroutine_threadsafe(set_it(), RE.loop).result()


@parametrize_suspenders
@requires_ophyd_async
def test_suspender_async_signal(klass, sc_args, start_val, fail_val, resume_val, wait_time, RE):
    sig = _connected_soft_signal(RE, start_val)

    def putter(val):
        _set_on_loop(RE, sig, val)

    _check_suspender(klass, sc_args, sig, putter, start_val, fail_val, resume_val, wait_time, RE)


@requires_ophyd_async
def test_pretripped_async_signal(RE):
    "Tests that install() sees the current value, as ophyd's subscribe(run=True) does"
    sig = _connected_soft_signal(RE, 1)
    susp = SuspendBoolHigh(sig)

    susp.install(RE)

    assert susp.tripped


@requires_ophyd_async
def test_suspender_plans_async_signal(RE):
    "Tests that an async suspender can be installed and removed via Msg"
    sig = _connected_soft_signal(RE, 0)
    my_suspender = SuspendBoolHigh(sig, sleep=0.2)
    scan = [Msg("checkpoint"), Msg("sleep", None, 0.2)]

    def trip_then_clear():
        threading.Timer(0.1, _set_on_loop, (RE, sig, 1)).start()
        threading.Timer(0.5, _set_on_loop, (RE, sig, 0)).start()

    # installed from inside a plan, it suspends and resumes
    trip_then_clear()
    start = ttime.time()
    RE([Msg("install_suspender", None, my_suspender)] + scan)
    assert ttime.time() - start > 0.4 + 0.2 + 0.2
    # and it is gone once that plan ends: installing from inside a plan is
    # ephemeral now, so it does not carry into the next one. See
    # test_suspender_installed_by_a_plan_ends_with_it for the lifetime itself.
    assert my_suspender not in RE.suspenders

    # Removing it from inside a plan takes it out of the next plan too.
    trip_then_clear()
    start = ttime.time()
    RE([Msg("remove_suspender", None, my_suspender)] + scan)
    assert ttime.time() - start < 0.5
    assert my_suspender not in RE.suspenders


@requires_ophyd_async
def test_event_type_is_rejected_for_a_subscribable_signal(RE):
    "A Subscribable signal has no event types, so asking for one must not be ignored"
    sig = _connected_soft_signal(RE, 0)
    susp = SuspendBoolHigh(sig)

    with pytest.raises(RuntimeError, match="event_type"):
        susp.install(RE, event_type="value")

    # The rejected install recorded nothing, so a valid one still goes through.
    # Asserted through the public route rather than an attribute, because what
    # a half-installed suspender would be holding differs between the RunEngine
    # this test was written against and the permit it holds now.
    RE.install_suspender(susp)
    RE.remove_suspender(susp)


@requires_ophyd_async
def test_suspend_when_changed_async_signal(RE):
    "expected_value cannot be read from a Subscribable signal until it is installed"
    sig = _connected_soft_signal(RE, 1)
    susp = SuspendWhenChanged(sig, allow_resume=True)
    assert susp.expected_value is None

    susp.install(RE)

    assert susp.expected_value == 1
    assert not susp.tripped

    _set_on_loop(RE, sig, 2)

    assert susp.tripped
    assert susp._get_justification() == 'Signal sig, got "2.0", expected "1.0"'


def test_pretripped(RE, hw):
    "Tests if suspender is tripped before __call__"
    sig = hw.bool_sig
    scan = [Msg("checkpoint")]
    msg_lst = []
    sig.put(1)

    def accum(msg):
        msg_lst.append(msg)

    susp = SuspendBoolHigh(sig)

    RE.install_suspender(susp)
    threading.Timer(1, sig.put, (0,)).start()
    RE.msg_hook = accum
    RE(scan)

    assert len(msg_lst) == 2
    assert ["wait_for", "checkpoint"] == [m[0] for m in msg_lst]


def test_suspender_wrapper(RE, hw):

    wait_time = 0.2
    sleep_time = 0.2
    trigger_time = 0.5

    sig = hw.bool_sig
    scan = [Msg("checkpoint"), Msg("sleep", None, sleep_time)]
    sig.put(0)

    susp = SuspendBoolHigh(sig, sleep=wait_time)

    RE(suspend_wrapper(scan, susp))
    assert RE.state == "idle"

    sig.put(1)
    threading.Timer(trigger_time, sig.put, (0,)).start()

    start = ttime.time()

    RE(suspend_wrapper(scan, susp))
    stop = ttime.time()
    delta = stop - start
    assert delta > trigger_time + wait_time + sleep_time


@pytest.mark.parametrize(
    "pre_plan,post_plan,expected_list",
    [
        (
            [Msg("null")],
            None,
            [
                "checkpoint",
                "sleep",
                "_start_suspender",
                "rewindable",
                "null",
                "wait_for",
                "_resume_from_suspender",
                "rewindable",
                "sleep",
            ],
        ),
        (
            None,
            [Msg("null")],
            [
                "checkpoint",
                "sleep",
                "_start_suspender",
                "rewindable",
                "wait_for",
                "_resume_from_suspender",
                "null",
                "rewindable",
                "sleep",
            ],
        ),
        (
            [Msg("null")],
            [Msg("null")],
            [
                "checkpoint",
                "sleep",
                "_start_suspender",
                "rewindable",
                "null",
                "wait_for",
                "_resume_from_suspender",
                "null",
                "rewindable",
                "sleep",
            ],
        ),
        (
            lambda: [Msg("null")],
            lambda: [Msg("null")],
            [
                "checkpoint",
                "sleep",
                "_start_suspender",
                "rewindable",
                "null",
                "wait_for",
                "_resume_from_suspender",
                "null",
                "rewindable",
                "sleep",
            ],
        ),
    ],
)
def test_pre_suspend_plan(RE, pre_plan, post_plan, expected_list, hw):
    sig = hw.bool_sig
    scan = [Msg("checkpoint"), Msg("sleep", None, 0.2)]
    msg_lst = []
    sig.put(0)

    def accum(msg):
        msg_lst.append(msg)

    susp = SuspendBoolHigh(sig, pre_plan=pre_plan, post_plan=post_plan)

    RE.install_suspender(susp)
    threading.Timer(0.1, sig.put, (1,)).start()
    threading.Timer(1, sig.put, (0,)).start()
    RE.msg_hook = accum
    RE(scan)

    assert len(msg_lst) == len(expected_list)
    assert expected_list == [m[0] for m in msg_lst]

    RE.remove_suspender(susp)
    RE(scan)
    assert susp not in RE.suspenders

    RE.install_suspender(susp)
    RE.clear_suspenders()
    assert susp not in RE.suspenders
    assert not RE.suspenders


def test_pause_from_suspend(RE, hw):
    "Tests what happens when a pause is requested from a suspended state"
    sig = hw.bool_sig
    scan = [Msg("checkpoint")]
    msg_lst = []
    sig.put(1)

    def accum(msg):
        msg_lst.append(msg)

    susp = SuspendBoolHigh(sig)

    RE.install_suspender(susp)
    threading.Timer(1, RE.request_pause).start()
    threading.Timer(2, sig.put, (0,)).start()
    RE.msg_hook = accum
    with pytest.raises(RunEngineInterrupted):
        RE(scan)
    assert [m[0] for m in msg_lst] == ["wait_for"]
    RE.resume()
    assert ["wait_for", "wait_for", "checkpoint"] == [m[0] for m in msg_lst]


def test_suspend_when_changed_latches_expected_value_at_install(RE, hw):
    "The default latches on install, whichever protocol the signal implements"
    sig = hw.bool_sig
    sig.put(1)

    susp = SuspendWhenChanged(sig, allow_resume=True)
    # __init__ does not read the signal.
    assert susp.expected_value is None

    RE.install_suspender(susp)
    try:
        # Latched from the reading install calls back with.
        assert susp.expected_value == 1
        assert not susp.tripped
    finally:
        RE.remove_suspender(susp)


def test_suspend_when_changed_preserves_falsy_expected_value(hw):
    sig = hw.bool_sig
    sig.put(1)

    susp = SuspendWhenChanged(sig, expected_value=0)

    assert susp.expected_value == 0
    assert not susp._should_suspend(0)
    assert susp._should_suspend(1)


def test_deferred_pause_from_suspend(RE, hw):
    "Tests what happens when a soft pause is requested from a suspended state"
    sig = hw.bool_sig
    scan = [Msg("checkpoint"), Msg("null")]
    msg_lst = []
    deferred_pause_event = threading.Event()
    waiting_event = threading.Event()
    sig.put(1)

    def accum(msg):
        if msg[0] == "wait_for":
            waiting_event.set()
        msg_lst.append(msg)

    def wait_then_request_pause():
        waiting_event.wait(timeout=5)
        assert waiting_event.is_set()
        RE.request_pause(True)
        deferred_pause_event.set()

    def wait_then_put():
        deferred_pause_event.wait(timeout=5)
        assert deferred_pause_event.is_set()
        sig.put(0)

    susp = SuspendBoolHigh(sig)

    RE.install_suspender(susp)
    threading.Thread(target=wait_then_request_pause, daemon=True).start()
    threading.Thread(target=wait_then_put, daemon=True).start()
    RE.msg_hook = accum
    with pytest.raises(RunEngineInterrupted):
        RE(scan)
    assert [m[0] for m in msg_lst] == ["wait_for", "checkpoint"]
    RE.resume()
    assert ["wait_for", "checkpoint", "null"] == [m[0] for m in msg_lst]


def test_unresumable_suspend_fail(RE):
    "Tests what happens when a soft pause is requested from a suspended state"

    scan = [Msg("clear_checkpoint"), Msg("sleep", None, 2)]
    m_coll = MsgCollector()
    RE.msg_hook = m_coll

    ev = _fabricate_asycio_event(RE.loop)
    threading.Timer(0.1, partial(suspend_until, RE, ev.wait)).start()
    threading.Timer(1, ev.set).start()
    start = time.time()
    with pytest.raises(RunEngineInterrupted):
        RE(scan)
    stop = time.time()
    assert 0.1 < stop - start < 1


def test_suspender_plans(RE, hw):
    "Tests that the suspenders can be installed via Msg"
    sig = hw.bool_sig
    my_suspender = SuspendBoolHigh(sig, sleep=0.2)

    def putter(val):
        sig.put(val)

    putter(0)

    # Do the messages work? A suspender a plan installs is that plan's: it
    # withholds that plan's permit and is unsubscribed when the plan ends,
    # so it is gone by the time RE(...) returns.
    seen = []

    def note_while_running():
        yield Msg("install_suspender", None, my_suspender)
        seen.append(my_suspender in RE.suspenders)
        yield Msg("remove_suspender", None, my_suspender)

    RE(note_while_running())
    # Installed while its own plan ran.
    assert seen == [True]
    # And gone once that plan ended.
    assert my_suspender not in RE.suspenders
    RE([Msg("remove_suspender", None, my_suspender)])
    assert my_suspender not in RE.suspenders

    # Can we call both in a plan?
    RE([Msg("install_suspender", None, my_suspender), Msg("remove_suspender", None, my_suspender)])

    scan = [Msg("checkpoint"), Msg("sleep", None, 0.2)]

    # No suspend scan: does the wrapper error out?
    start = ttime.time()
    RE(suspend_wrapper(scan, my_suspender))
    stop = ttime.time()
    delta = stop - start
    assert delta < 0.9

    # Suspend scan
    start = ttime.time()
    threading.Timer(0.1, putter, (1,)).start()
    threading.Timer(0.5, putter, (0,)).start()
    RE(suspend_wrapper(scan, my_suspender))
    stop = ttime.time()
    delta = stop - start
    assert delta > 0.9

    # Did we clean up?
    start = ttime.time()
    threading.Timer(0.1, putter, (1,)).start()
    threading.Timer(0.5, putter, (0,)).start()
    RE(scan)
    stop = ttime.time()
    delta = stop - start
    assert delta < 0.9


def test_two_conditions_make_one_suspension(RE):
    """Two conditions going bad at once suspend the plan once, not once each.

    The reasons accumulate on one permit, so the plan rewinds once while each
    condition's pre-plan runs as it fires -- which is why pre- and post-plans
    must be idempotent.
    """
    sig_a = Signal(value=0, name="sig_a")
    sig_b = Signal(value=0, name="sig_b")
    susp_a = SuspendBoolHigh(sig_a, pre_plan=[Msg("null")], post_plan=[Msg("null")])
    susp_b = SuspendBoolHigh(sig_b, pre_plan=[Msg("null")], post_plan=[Msg("null")])
    RE.install_suspender(susp_a)
    RE.install_suspender(susp_b)

    m_coll = MsgCollector()
    RE.msg_hook = m_coll

    threading.Timer(0.2, sig_a.put, (1,)).start()
    threading.Timer(0.25, sig_b.put, (1,)).start()
    threading.Timer(0.8, sig_a.put, (0,)).start()
    threading.Timer(0.85, sig_b.put, (0,)).start()

    RE([Msg("checkpoint"), Msg("sleep", None, 0.5), Msg("null")])

    commands = [msg.command for msg in m_coll.msgs]
    # One suspension for both conditions.
    assert commands.count("_start_suspender") == 1
    # And the plan waits once.
    assert commands.count("wait_for") == 1
    RE.clear_suspenders()


def test_trip_while_paused_holds_the_plan_on_resume(RE, hw):
    """A condition that goes bad while the plan is paused holds it on resume.

    Held, not suspended. A pause hands control back to the user, so returning
    from one is like returning from idle: the plan waits for permission and runs
    no pre-plans, because the user may well have opened the shutter themselves
    and there is nothing a pre-plan should be reversing.
    """
    sig = hw.bool_sig
    sig.put(0)
    ran = []

    def note(tag):
        def plan():
            ran.append(tag)
            yield Msg("null")

        return plan

    susp = SuspendBoolHigh(sig, pre_plan=note("pre"), post_plan=note("post"))
    RE.install_suspender(susp)

    m_coll = MsgCollector()
    RE.msg_hook = m_coll

    threading.Timer(0.2, RE.request_pause).start()
    with pytest.raises(RunEngineInterrupted):
        RE([Msg("checkpoint"), Msg("sleep", None, 1), Msg("null")])
    assert RE.state == "paused"

    sig.put(1)
    ttime.sleep(0.3)
    # The condition really is bad.
    assert susp.tripped
    # And nothing ran while the user had control.
    assert ran == []

    threading.Timer(0.5, sig.put, (0,)).start()
    start = ttime.time()
    RE.resume()
    elapsed = ttime.time() - start

    # Resuming waited for the condition to clear.
    assert elapsed > 0.4
    commands = [msg.command for msg in m_coll.msgs]
    # Held, rather than suspended.
    assert "_start_suspender" not in commands
    # And neither plan ran on the way back in.
    assert ran == []
    RE.clear_suspenders()


def test_retrip_inside_sleep_does_not_release_early(RE, hw):
    """Characterization test: a condition that recovers and goes bad again
    inside the suspender's ``sleep`` window holds the plan until the *second*
    recovery has settled.

    This one must keep passing. The release a recovery schedules is a timer,
    and the risk when suspension state is shared rather than per-suspension is
    that the older timer comes due and drops the newer condition's hold.
    """
    sig = hw.bool_sig
    sig.put(0)
    susp = SuspendBoolHigh(sig, sleep=0.5)
    RE.install_suspender(susp)

    threading.Timer(0.1, sig.put, (1,)).start()  # goes bad
    threading.Timer(0.3, sig.put, (0,)).start()  # recovers: release due at 0.8
    threading.Timer(0.4, sig.put, (1,)).start()  # goes bad again, inside the window
    threading.Timer(1.0, sig.put, (0,)).start()  # recovers for good: release due at 1.5

    start = ttime.time()
    RE([Msg("checkpoint"), Msg("sleep", None, 0.1), Msg("null")])
    elapsed = ttime.time() - start

    # The release scheduled by the first recovery must not free the plan.
    assert elapsed > 1.4


def test_suspender_installed_by_a_plan_ends_with_it(RE, hw):
    """A suspender a plan installs for itself is visible while that plan runs
    and gone once it ends.

    It withholds the plan's own permit and is released with the plan.
    ``RE.suspenders`` reports it meanwhile, being the union of the durable
    suspenders and the running plan's.
    """
    sig = hw.bool_sig
    sig.put(0)
    susp = SuspendBoolHigh(sig)
    seen = []

    def note():
        yield Msg("install_suspender", None, susp)
        seen.append(("after install", susp in RE.suspenders))
        yield Msg("remove_suspender", None, susp)
        seen.append(("after remove", susp in RE.suspenders))
        yield Msg("install_suspender", None, susp)

    RE(note())

    assert seen == [("after install", True), ("after remove", False)]
    # The plan's own suspenders end with the plan.
    assert susp not in RE.suspenders


def test_clear_suspenders_while_paused_then_resume(RE, hw):
    """Characterization test: the escape hatch beamline staff actually use.

    Beam goes down, the plan suspends, the user interrupts to get a prompt,
    clears the suspenders and resumes::

        RE(my_plan())
        C-c
        RE.clear_suspenders()
        RE.resume()

    Clearing must both uninstall the suspender and release the hold it has on
    the plan, or the resumed plan waits forever with nothing left to free it.
    """
    sig = hw.bool_sig
    sig.put(1)  # beam is already down
    susp = SuspendBoolHigh(sig)
    RE.install_suspender(susp)

    m_coll = MsgCollector()
    RE.msg_hook = m_coll

    threading.Timer(0.5, RE.request_pause).start()
    with pytest.raises(RunEngineInterrupted):
        RE([Msg("checkpoint"), Msg("null")])
    assert RE.state == "paused"
    # Still held by the condition.
    assert susp.tripped

    RE.clear_suspenders()
    assert RE.suspenders == ()

    RE.resume()
    # The plan ran to the end rather than waiting forever.
    assert RE.state == "idle"
    assert [msg.command for msg in m_coll.msgs][-1] == "null"
    sig.put(0)
