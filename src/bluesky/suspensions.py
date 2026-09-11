"""A plan's suspension, and the reasons that trip it."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Hashable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from .utils import Msg

PlanLike = Iterable[Msg] | Callable[[], Iterable[Msg]]


@dataclass(frozen=True)
class SuspensionReason:
    """One reason a suspension is tripped, and the plans to run around it.

    One per condition. They are not merged: each condition runs its own
    pre-plan as it fires, so whatever is supervising needs them apart.
    """

    justification: str
    pre_plan: PlanLike | None = None
    post_plan: PlanLike | None = None


def join_justifications(reasons: Mapping[Hashable, SuspensionReason]) -> str:
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


class Suspension:
    """What holds a plan up, tripped while anything has a reason to trip it.

    Reasons are keyed, normally by the suspender that raised them, and the
    nothing is tripped exactly when none stands. Two conditions tripping at once
    are two reasons and one suspension, rather than two suspensions.

    Suspensions chain. One with a ``parent`` is tripped whenever its parent
    is, which is how a suspender installed somewhere long-lived holds up every
    plan run under it while one installed by a plan holds up only that plan.

    Written only on the loop, and read from anywhere. `trip` and `clear`
    do not check that: this class is internal, and its callers are the two
    boundaries that own their crossings -- a suspender tripping on its signal's
    thread, and the `RunEngine` methods called from the prompt.

    `tripped` and `reasons` answer on any thread. The reasons are an
    immutable mapping, swapped rather than mutated, so a reader sees one
    snapshot or the next and never a mapping mid-change. Their callers report
    rather than decide, so eventual consistency is what they need.
    """

    def __init__(self, name: str, loop: asyncio.AbstractEventLoop, parent: Suspension | None = None) -> None:
        self.name = name
        self._loop = loop
        self._parent = parent
        # Immutable, and replaced wholesale rather than mutated in place.
        # A reader off the loop -- `RunEngine.suspenders` and
        # `PlanSession.suspensions` are read from whatever thread asks -- then
        # sees one snapshot or the next and never a mapping mid-change. It also
        # costs nothing: reasons change when a suspender trips, not per message.
        self._reasons: Mapping[Hashable, SuspensionReason] = MappingProxyType({})
        # Pending delayed clears, so that a key tripping again cancels the
        # release its own recovery scheduled. Without this a signal that
        # recovers and trips again inside the settle-down time has the older
        # release come due and drop the newer reason.
        self._releases: dict[Hashable, asyncio.TimerHandle] = {}
        # Set-and-cleared on every change anywhere in the chain. Shared with
        # the parent rather than owned, because this suspension is tripped by its
        # own reasons *or* its parent's, so a waiter here has to be woken by a
        # change up there. Sharing gets that without the parent holding any
        # reference to its children: a child reaches up, as it already does for
        # `tripped` and `reasons`, and nothing reaches down.
        self._changed: asyncio.Event = parent._changed if parent is not None else asyncio.Event()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The loop this suspension's state lives on.

        Public because crossing onto it is the caller's job: a suspender trips
        on whatever thread its signal calls back on and owns that hop.
        """
        return self._loop

    def __repr__(self) -> str:
        state = f"tripped by {len(self.reasons)}" if self.tripped else "clear"
        return f"<{type(self).__name__} {self.name!r} {state}>"

    @property
    def tripped(self) -> bool:
        """Whether anything is holding the plan up, here or above.

        Derived from `reasons` rather than tracked, so the two cannot disagree.
        """
        return bool(self.reasons)

    @property
    def reasons(self) -> Mapping[Hashable, SuspensionReason]:
        """Every reason this suspension is tripped, keyed by whoever tripped it.

        Includes the chain above, outermost suspension first, because that is the
        order a suspension runs pre-plans in and the reverse of the order it
        runs post-plans in. Empty exactly when nothing is tripped.
        """
        if self._parent is None:
            return self._reasons
        return MappingProxyType({**self._parent.reasons, **self._reasons})

    def trip(
        self,
        key: Hashable,
        justification: str,
        *,
        pre_plan: PlanLike | None = None,
        post_plan: PlanLike | None = None,
    ) -> None:
        """Record that ``key`` has tripped. Loop thread only."""
        release = self._releases.pop(key, None)
        if release is not None:
            release.cancel()
        self._reasons = MappingProxyType(
            {**self._reasons, key: SuspensionReason(justification, pre_plan, post_plan)}
        )
        self._notify_changed()

    def clear(self, key: Hashable, *, after: float = 0) -> None:
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
        self._notify_changed()

    def _notify_changed(self) -> None:
        """Wake everything waiting on this chain. Loop thread only.

        `set` wakes every waiter parked right now, and `clear` immediately after
        leaves the flag down for the next one -- so waking means "something in
        the chain moved" and nothing more, and every waiter re-tests the
        condition it actually cares about.
        """
        self._changed.set()
        self._changed.clear()

    async def wait_changed(self) -> None:
        """Wait until a reason is raised or dropped, anywhere in the chain.

        Callers must not await between testing their condition and calling
        this, or they can miss the edge that would have woken them. Every
        caller here is a `while <condition>: await wait_changed()` loop, where
        the test and this call are one uninterrupted stretch of loop thread,
        so no change can slip between them.
        """
        await self._changed.wait()

    async def wait_cleared(self) -> None:
        """Wait until no reason stands in the chain."""
        while self.tripped:
            await self.wait_changed()
