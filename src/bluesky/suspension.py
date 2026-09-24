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
    """One reason a suspension is tripped, with its pre- and post-plans."""

    justification: str
    pre_plan: PlanLike | None = None
    post_plan: PlanLike | None = None


def join_justifications(reasons: Mapping[Hashable, SuspensionReason]) -> str:
    """Every standing reason's justification, one per line, outermost first."""
    return "\n".join(reason.justification for reason in reasons.values() if reason.justification)


class Suspension:
    """Tripped while any reason stands, here or in a parent.

    Reasons are keyed, normally by the suspender that tripped them.
    `trip` and `clear` are loop-only and unchecked; `tripped` and `reasons`
    are safe on any thread.
    """

    def __init__(self, name: str, loop: asyncio.AbstractEventLoop, parent: Suspension | None = None) -> None:
        self.name = name
        self._loop = loop
        self._parent = parent
        # Immutable and swapped whole, so readers off the loop see a snapshot.
        self._reasons: Mapping[Hashable, SuspensionReason] = MappingProxyType({})
        # Pending delayed clears, cancelled if the key trips again first.
        self._releases: dict[Hashable, asyncio.TimerHandle] = {}
        # Shared with the parent, so a change up the chain wakes waiters here.
        self._changed: asyncio.Event = parent._changed if parent is not None else asyncio.Event()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The loop this suspension's state lives on."""
        return self._loop

    def __repr__(self) -> str:
        state = f"tripped by {len(self.reasons)}" if self.tripped else "clear"
        return f"<{type(self).__name__} {self.name!r} {state}>"

    @property
    def tripped(self) -> bool:
        """Whether any reason stands, here or above."""
        return bool(self.reasons)

    @property
    def reasons(self) -> Mapping[Hashable, SuspensionReason]:
        """Every standing reason, keyed; the parent's first."""
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
            # A timer must be scheduled on its own loop, hence loop-only.
            self._releases[key] = self._loop.call_later(after, self._release, key)
        else:
            self._release(key)

    def _release(self, key: Hashable) -> None:
        self._releases.pop(key, None)
        if key in self._reasons:
            self._reasons = MappingProxyType({k: v for k, v in self._reasons.items() if k != key})
        self._notify_changed()

    def _notify_changed(self) -> None:
        """Wake every waiter on this chain. Loop thread only."""
        self._changed.set()
        self._changed.clear()

    async def wait_changed(self) -> None:
        """Wait until a reason is raised or dropped anywhere in the chain.

        Do not await between testing a condition and calling this.
        """
        await self._changed.wait()

    async def wait_cleared(self) -> None:
        """Wait until no reason stands in the chain."""
        while self.tripped:
            await self.wait_changed()
