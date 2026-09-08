"""Permission to run, and the reasons it may be withheld."""

from __future__ import annotations

import asyncio
import threading
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
        # Pulsed on every change anywhere in the chain. One event and not one
        # per edge: every wait here is a `while <condition>` loop over it, so
        # the edge a caller cares about is the condition it tests, and
        # `granted` -- which also asks the parent -- is the only state. Shared
        # with the parent rather than composed with it at each wait, so that a
        # wait is one await on one event however deep the chain runs.
        self._pulse = parent._pulse if parent is not None else _Pulse()

    def __repr__(self) -> str:
        state = "granted" if self.granted else f"withheld by {len(self.withheld_by)}"
        return f"<{type(self).__name__} {self.name!r} {state}>"

    @property
    def granted(self) -> bool:
        """Whether the plan may run: nothing is withholding it, here or above.

        Derived rather than tracked, so that it cannot disagree with
        `withheld_by`. Two independent walks of the chain could return a
        verdict and a set of reasons that did not match, and whoever read both
        had to reconcile them.

        This walks to the root and merges on every call where the short-circuit
        it replaced did not. Chains are two deep -- a session's permit and the
        running plan's -- so that is one merge of two small mappings, and it
        happens once per pulse rather than per message. Anything deeper would
        want a loop-side fast path, not a second public accessor.
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
        """Withhold on ``key``'s behalf until granted. Callable from any thread."""
        release = self._releases.pop(key, None)
        if release is not None:
            release.cancel()
        self._reasons = MappingProxyType({**self._reasons, key: Suspension(justification, pre_plan, post_plan)})
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
        if key in self._reasons:
            self._reasons = MappingProxyType({k: v for k, v in self._reasons.items() if k != key})
        self._tell_the_loop()

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
        self._pulse.fire()
