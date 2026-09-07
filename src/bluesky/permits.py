"""Permission to run, and the reasons it may be withheld."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Hashable, Iterable, Mapping
from dataclasses import dataclass

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


class Permit:
    """Permission to run, withheld while anything has a reason to withhold it.

    Reasons are keyed, normally by the suspender that raised them, and the
    permit is granted exactly when none stands. Two conditions tripping at once
    are two reasons and one suspension, rather than two suspensions.

    Permits chain. A permit with a ``parent`` is withheld whenever its parent
    is, which is how a suspender installed somewhere long-lived holds up every
    plan run under it while one installed by a plan holds up only that plan.

    The reasons are the state, and `withhold` and `grant` may be called from any
    thread: a suspender trips on whatever thread its signal calls back on, and
    whether a permit is granted has to be true for that thread the moment it
    says so, or a plan built between the trip and the loop noticing it would
    start unheld. Telling the loop is this class's own business -- nothing
    outside can forget to do it.
    """

    def __init__(self, name: str, loop: asyncio.AbstractEventLoop, parent: Permit | None = None) -> None:
        self.name = name
        self._loop = loop
        self._parent = parent
        self._reasons: dict[Hashable, Suspension] = {}
        # Pending delayed grants, so that a key withholding again cancels the
        # release its own recovery scheduled. Without this a signal that
        # recovers and trips again inside the settle-down time has the older
        # release come due and drop the newer reason.
        self._releases: dict[Hashable, asyncio.TimerHandle] = {}
        # Pulsed on every change to this permit's own reasons. One event and
        # not one per edge: every wait here is a `while <condition>` loop over
        # it, so the edge a caller cares about is the condition it tests, and
        # `granted` -- which also asks the parent -- is the only state.
        self._changed = asyncio.Event()

    def __repr__(self) -> str:
        state = "granted" if self.granted else f"withheld by {len(self._reasons)}"
        return f"<{type(self).__name__} {self.name!r} {state}>"

    @property
    def granted(self) -> bool:
        """Whether the plan may run: no reason here, and none above."""
        if self._reasons:
            return False
        return self._parent.granted if self._parent is not None else True

    @property
    def reasons(self) -> dict[Hashable, Suspension]:
        """Every reason standing in the chain, keyed by whoever raised it.

        In the order they were raised, outermost permit first, because a
        suspension runs pre-plans in that order and post-plans in reverse.
        """
        above = self._parent.reasons if self._parent is not None else {}
        return {**above, **self._reasons}

    def withhold(
        self,
        key: Hashable,
        justification: str,
        *,
        pre_plan: PlanLike | None = None,
        post_plan: PlanLike | None = None,
    ) -> None:
        """Withhold on ``key``'s behalf until granted. Callable from any thread."""
        release = self._releases.pop(key, None)
        if release is not None:
            release.cancel()
        self._reasons[key] = Suspension(justification, pre_plan, post_plan)
        self._tell_the_loop()

    def grant(self, key: Hashable, *, after: float = 0) -> None:
        """Drop ``key``'s reason, ``after`` seconds from now. Callable from any thread."""
        if after and self._loop.is_running():
            self._on_loop(
                lambda: self._releases.__setitem__(key, self._loop.call_later(after, self._release, key))
            )
        else:
            # Nothing would fire the timer if the loop is not running, and the
            # reason would outlive the condition that raised it.
            self._release(key)

    def _release(self, key: Hashable) -> None:
        self._releases.pop(key, None)
        self._reasons.pop(key, None)
        self._tell_the_loop()

    async def wait_changed(self) -> None:
        """Wait until a reason is raised or dropped, anywhere in the chain."""
        waiters: list[asyncio.Future] = [asyncio.ensure_future(self._changed.wait())]
        if self._parent is not None:
            waiters.append(asyncio.ensure_future(self._parent.wait_changed()))
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()
        self._changed.clear()

    async def wait_granted(self) -> None:
        """Wait until no reason stands in the chain."""
        while not self.granted:
            await self.wait_changed()

    def _tell_the_loop(self) -> None:
        """Bring the loop's view of this permit into step."""
        self._on_loop(self._sync)

    def _on_loop(self, func: Callable[[], None]) -> None:
        if threading.get_ident() == getattr(self._loop, "_thread_id", None) or not self._loop.is_running():
            func()
        else:
            self._loop.call_soon_threadsafe(func)

    def _sync(self) -> None:
        """Bring the loop's view of this permit's own reasons into step."""
        self._changed.set()
