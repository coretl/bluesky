"""Permission to run, and the reasons it may be withheld."""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass

from .utils import Msg, ensure_generator

PlanLike = Iterable[Msg] | Callable[[], Iterable[Msg]]


@dataclass(frozen=True)
class Suspension:
    """Why a permit is withheld, and the plans to run around the wait.

    One reason or several merged into one look the same to everything
    downstream, so a plan held up by two conditions is suspended once.
    """

    justification: str
    pre_plan: PlanLike | None = None
    post_plan: PlanLike | None = None


def _chain(plans: list[PlanLike]) -> PlanLike | None:
    """Compose several pre- or post-plans into one, or None if there are none."""
    plans = [plan for plan in plans if plan is not None]
    if not plans:
        return None

    def chained():
        for plan in plans:
            yield from ensure_generator(plan() if callable(plan) else plan)

    return chained


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
        # Both edges, because an asyncio.Event can only be awaited for being
        # set, and the executor needs to wait for either direction.
        self._is_granted = asyncio.Event()
        self._is_withheld = asyncio.Event()
        self._is_granted.set()
        self._children: weakref.WeakSet[Permit] = weakref.WeakSet()
        if parent is not None:
            parent._children.add(self)

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
    def suspension(self) -> Suspension | None:
        """Every reason standing in the chain merged into one, outermost first."""
        reasons = self._chain_reasons()
        if not reasons:
            return None
        return Suspension(
            justification="\n".join(r.justification for r in reasons if r.justification),
            pre_plan=_chain([r.pre_plan for r in reasons]),
            post_plan=_chain([r.post_plan for r in reasons]),
        )

    def _chain_reasons(self) -> list[Suspension]:
        """This permit's reasons, with everything above it in front."""
        above = self._parent._chain_reasons() if self._parent is not None else []
        return above + list(self._reasons.values())

    def withhold(
        self,
        key: Hashable,
        justification: str = "",
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

    async def wait_granted(self) -> None:
        """Wait until no reason stands in the chain."""
        while not self.granted:
            self._sync()
            await self._is_granted.wait()

    async def wait_withheld(self) -> None:
        """Wait until some reason stands in the chain."""
        while self.granted:
            self._sync()
            await self._is_withheld.wait()

    def _tell_the_loop(self) -> None:
        """Bring the loop's view of this permit, and its children's, into step."""
        self._on_loop(self._sync)

    def _on_loop(self, func: Callable[[], None]) -> None:
        if threading.get_ident() == getattr(self._loop, "_thread_id", None) or not self._loop.is_running():
            func()
        else:
            self._loop.call_soon_threadsafe(func)

    def _sync(self) -> None:
        if self.granted:
            self._is_withheld.clear()
            self._is_granted.set()
        else:
            self._is_granted.clear()
            self._is_withheld.set()
        for child in list(self._children):
            child._sync()
