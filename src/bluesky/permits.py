"""Permission to run, and the reasons it may be withheld."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Hashable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from .utils import Msg

PlanLike = Iterable[Msg] | Callable[[], Iterable[Msg]]


@dataclass(frozen=True)
class Suspension:
    """Why a permit is withheld, and the plans to run around the wait.

    One per condition. They are not merged: each condition runs its own
    pre-plan as it fires, so whatever is supervising needs them apart.
    """

    justification: str
    pre_plan: PlanLike | None = None
    post_plan: PlanLike | None = None


def join_justifications(reasons: Mapping[Hashable, Suspension]) -> str:
    """Every standing reason's justification, one per line, outermost first."""
    return "\n".join(reason.justification for reason in reasons.values() if reason.justification)


def running_on(loop: asyncio.AbstractEventLoop) -> bool:
    """Whether this thread is the one running ``loop``.

    One spelling of a question asked in several places. `asyncio.get_running_loop`
    is public and exact, where the ``getattr(loop, "_thread_id", ...)`` this
    replaces reached for a private CPython attribute and had to pick a default
    for loops that lack it -- a default that decided, in opposite directions in
    different files, what happens when the check cannot be made at all.
    """
    try:
        return asyncio.get_running_loop() is loop
    except RuntimeError:
        return False


class _Pulse:
    """A broadcast edge, shared by every permit in a chain.

    One `asyncio.Event`, and nobody clears it: `fire` swaps a fresh event in
    and sets the old one, so every waiter parked on it wakes and no waiter can
    consume the edge out from under another. Waking on a pulse means "something
    in the chain moved" and nothing more -- the caller re-tests the condition
    it actually cares about, which is what the `while` around every wait here
    already does.

    The chain shares one of these because a permit is withheld by its own
    reasons *or* its parent's, so a waiter on a child has to be woken by a
    change at the parent. Sharing the parent's pulse gets that without the
    parent holding any reference to its children: a child reaches up, as it
    already does for `granted` and `withheld_by`, and nothing reaches down.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def wait(self) -> Coroutine[Any, Any, bool]:
        """Park until the next `fire`.

        The event is bound now, not when the coroutine is first stepped, so a
        pulse between this call and that step still wakes it: it sets the very
        event this coroutine is holding.
        """
        return self._event.wait()

    def fire(self) -> None:
        """Wake everything parked on the chain. Loop thread only."""
        event, self._event = self._event, asyncio.Event()
        event.set()


class Permit:
    """Permission to run, withheld while anything has a reason to withhold it.

    Reasons are keyed, normally by the suspender that raised them, and the
    permit is granted exactly when none stands. Two conditions tripping at once
    are two reasons and one suspension, rather than two suspensions.

    Permits chain. A permit with a ``parent`` is withheld whenever its parent
    is, which is how a suspender installed somewhere long-lived holds up every
    plan run under it while one installed by a plan holds up only that plan.

    Written only on the loop, and read from anywhere. `withhold` and `grant`
    do not check that: this class is internal, and its callers are the two
    boundaries that own their crossings -- a suspender tripping on its signal's
    thread, and the `RunEngine` methods called from the prompt.

    `granted` and `withheld_by` answer on any thread. The reasons are an
    immutable mapping, swapped rather than mutated, so a reader sees one
    snapshot or the next and never a mapping mid-change. Their callers report
    rather than decide, so eventual consistency is what they need.
    """

    def __init__(self, name: str, loop: asyncio.AbstractEventLoop, parent: Permit | None = None) -> None:
        self.name = name
        self._loop = loop
        self._parent = parent
        # Immutable, and replaced wholesale rather than mutated in place.
        # A reader off the loop -- `RunEngine.suspenders` and
        # `PlanSession.suspensions` are read from whatever thread asks -- then
        # sees one snapshot or the next and never a mapping mid-change. It also
        # costs nothing: reasons change when a suspender trips, not per message.
        self._reasons: Mapping[Hashable, Suspension] = MappingProxyType({})
        # Pending delayed grants, so that a key withholding again cancels the
        # release its own recovery scheduled. Without this a signal that
        # recovers and trips again inside the settle-down time has the older
        # release come due and drop the newer reason.
        self._releases: dict[Hashable, asyncio.TimerHandle] = {}
        # Pulsed on every change anywhere in the chain, and shared with the
        # parent, so a wait is one await on one event however deep the chain.
        self._pulse: _Pulse = parent._pulse if parent is not None else _Pulse()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The loop this permit's state lives on.

        Public because crossing onto it is the caller's job: a suspender trips
        on whatever thread its signal calls back on and owns that hop.
        """
        return self._loop

    def __repr__(self) -> str:
        state = "granted" if self.granted else f"withheld by {len(self.withheld_by)}"
        return f"<{type(self).__name__} {self.name!r} {state}>"

    @property
    def granted(self) -> bool:
        """Whether the plan may run: nothing is withholding it, here or above.

        Derived from `withheld_by` rather than tracked, so the two cannot
        disagree. Chains are two deep -- a session's permit and the running
        plan's -- so this is one merge of two small mappings, once per pulse
        rather than per message.
        """
        return not self.withheld_by

    @property
    def withheld_by(self) -> Mapping[Hashable, Suspension]:
        """Everything withholding this permit, keyed by whoever withheld it.

        Includes the chain above, outermost permit first, because that is the
        order a suspension runs pre-plans in and the reverse of the order it
        runs post-plans in. Empty exactly when the permit is granted.
        """
        if self._parent is None:
            return self._reasons
        return MappingProxyType({**self._parent.withheld_by, **self._reasons})

    def withhold(
        self,
        key: Hashable,
        justification: str,
        *,
        pre_plan: PlanLike | None = None,
        post_plan: PlanLike | None = None,
    ) -> None:
        """Withhold on ``key``'s behalf until granted. Loop thread only."""
        release = self._releases.pop(key, None)
        if release is not None:
            release.cancel()
        self._reasons = MappingProxyType({**self._reasons, key: Suspension(justification, pre_plan, post_plan)})
        self._pulse.fire()

    def grant(self, key: Hashable, *, after: float = 0) -> None:
        """Drop ``key``'s reason, ``after`` seconds from now. Loop thread only."""
        if after:
            # Being on the loop is what makes this safe, and is also what makes
            # it possible: a timer belongs to the loop that scheduled it, and a
            # loop we are running on is by definition running to fire it.
            self._releases[key] = self._loop.call_later(after, self._release, key)
        else:
            self._release(key)

    def _release(self, key: Hashable) -> None:
        self._releases.pop(key, None)
        if key in self._reasons:
            self._reasons = MappingProxyType({k: v for k, v in self._reasons.items() if k != key})
        self._pulse.fire()

    async def wait_changed(self) -> None:
        """Wait until a reason is raised or dropped, anywhere in the chain.

        Callers must not await between testing their condition and calling
        this, or they can miss the edge that would have woken them. Every
        caller here is a `while <condition>: await wait_changed()` loop, where
        the test and this call are one uninterrupted stretch of loop thread,
        so no pulse can slip between them.
        """
        await self._pulse.wait()

    async def wait_granted(self) -> None:
        """Wait until no reason stands in the chain."""
        while not self.granted:
            await self.wait_changed()
