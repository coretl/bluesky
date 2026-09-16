"""Tests for the seam between PlanSession, PlanRunner and RunEngine.

These guard the properties that make the runner usable without a
RunEngine: that it is pure asyncio, and that it can be driven directly.

Nothing here runs a plan yet. A runner is built by `PlanSession.start`, which
arrives in the next commit, so what can be pinned at this point is what the
module is rather than what it does -- and that is worth pinning here, because
these are the properties the move was for.
"""

import inspect
import pathlib
import threading

import pytest

import bluesky
from bluesky.plan_runner import PlanRunner


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


@pytest.mark.parametrize("cls", [PlanRunner])
def test_source_takes_no_locks(cls):
    """The runner never blocks a thread, so it may not lock or join."""
    source = inspect.getsource(cls)
    for forbidden in ("threading.", "_state_lock", ".acquire(", ".join("):
        assert forbidden not in source


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


def test_the_runner_never_says_what_to_press():
    """It cannot know that a keyboard is attached.

    the announce hook may be wired to a websocket, where telling someone to hit
    Ctrl-C is wrong. Saying what to press belongs to the `RunEngine` and to
    `SigintHandler`, which exist only where a terminal does.
    """
    source = (pathlib.Path(bluesky.__file__).parent / "plan_runner.py").read_text()

    # Not even in a docstring; SIGINT is the accurate word here.
    assert "Ctrl" not in source
