"""Public external interface of the classes that make up the rewrite."""

# Reasoning, decisions and open questions: .claude/notes/runengine-split-interface.md

from __future__ import annotations

import asyncio
from collections.abc import Callable, Collection, Hashable, Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from logging import LoggerAdapter
from typing import Any, Protocol, runtime_checkable

from bluesky.bundlers import RunBundler
from bluesky.plan_executor import RunEngineResult
from bluesky.protocols import Subscribable, SyncOrAsync
from bluesky.utils import Msg

PlanLike = Iterable[Msg] | Callable[[], Iterable[Msg]]
Metadata = MutableMapping[str, Any]
SubsLike = Callable | Sequence[Callable] | Mapping[str, Callable | Sequence[Callable]]


@runtime_checkable
class OphydSubscribable(Protocol):
    """An ophyd-style signal, whose subscribe takes an event type and replays."""

    def subscribe(
        self, function: Callable[..., None], *, event_type: str | None = None, run: bool = True
    ) -> None: ...

    def clear_sub(self, function: Callable[..., None]) -> None: ...


SignalLike = Subscribable | OphydSubscribable


class Dispatcher:
    """Sends each document to the subscribers registered for it."""

    def __init__(self, parent: Dispatcher | None = None, *, ignore_exceptions: bool = False) -> None:
        """A dispatcher whose documents reach ``parent``'s subscribers first."""

    def process(self, name: str, doc: dict[str, Any]) -> None:
        """Send ``doc`` to ``parent``'s subscribers, then to this one's."""

    def subscribe(self, func: Callable, name: str = "all") -> int:
        """Call ``func`` with every matching document, and return a token."""

    def unsubscribe(self, token: int) -> None:
        """Stop calling what ``subscribe`` returned this token for."""


@dataclass(frozen=True)
class Suspension:
    """Why a permit is withheld, and the plans to run around the wait."""

    justification: str
    pre_plan: PlanLike | None = None
    post_plan: PlanLike | None = None


class Permit:
    """Permission to run, withheld while anything has a reason to withhold it."""

    def __init__(self, name: str, loop: asyncio.AbstractEventLoop, parent: Permit | None = None) -> None:
        """A permit on ``loop``, granted unless it or ``parent`` is withheld."""

    @property
    def granted(self) -> bool:
        """Whether the plan may run: no reason here, and none above."""

    @property
    def reasons(self) -> dict[Hashable, Suspension]:
        """Every reason standing in the chain, outermost first, by who raised it."""

    def withhold(
        self,
        key: Hashable,
        justification: str,
        *,
        pre_plan: PlanLike | None = None,
        post_plan: PlanLike | None = None,
    ) -> None:
        """Withhold on ``key``'s behalf until granted. Callable from any thread."""

    def grant(self, key: Hashable, *, after: float = 0) -> None:
        """Drop ``key``'s reason, ``after`` seconds from now. Callable from any thread."""

    async def wait_changed(self) -> None:
        """Wait until a reason is raised or dropped, anywhere in the chain."""

    async def wait_granted(self) -> None:
        """Wait until no reason stands in the chain."""


class SuspenderBase:
    """Watch a signal, and withhold a permit while it reads as bad."""

    def __init__(
        self,
        signal: SignalLike,
        *,
        sleep: float = 0,
        pre_plan: PlanLike | None = None,
        post_plan: PlanLike | None = None,
        tripped_message: str = "",
    ) -> None:
        """Watch ``signal``, waiting ``sleep`` after recovery before releasing."""

    def install(self, permit: Permit, *, event_type: str | None = None) -> None:
        """Subscribe on the permit's loop, and withhold it while the signal reads as bad."""

    def remove(self) -> None:
        """Unsubscribe from the signal, and grant back whatever it was withholding."""

    def __call__(self, value: Any, **kwargs: Any) -> None:
        """Ophyd's callback: withhold or grant, on whatever thread the signal uses."""

    @property
    def tripped(self) -> bool:
        """Whether the condition is bad right now."""


@dataclass
class PlanHooks:
    """Where a plan's progress can be watched from, live."""

    msg_hook: Callable[[Msg], None] | None = None
    """Called with each ``Msg`` before it is processed."""

    waiting_hook: Callable[[Any], None] | None = None
    """Called with the statuses a plan is waiting on, and ``None`` when it stops."""

    state_hook: Callable[[str, str], None] | None = None
    """Called with the new and old state on every state change."""

    on_pause: Callable[[], None] | None = None
    """Called with no arguments once a plan has come to rest paused."""


@dataclass(frozen=True)
class PlanEnvironment:
    """Everything a running plan reads about where it is being run."""

    loop: asyncio.AbstractEventLoop
    """The event loop the plan is executed on."""

    log: LoggerAdapter
    """Where the plan logs to."""

    md: Metadata
    """Metadata for this plan, snapshotted as its executor is built."""

    next_scan_id: Callable[[], SyncOrAsync[int]]
    """Allocates the ``scan_id`` for a run that is opening, and returns it."""

    md_validator: Callable[[dict[str, Any]], None]
    """Raises to stop a run starting."""

    md_normalizer: Callable[[dict[str, Any]], dict[str, Any]]
    """Returns the metadata a run will actually record."""

    run_bundler_cls: type[RunBundler]
    """Composes the documents for each open run."""

    record_interruptions: bool
    """Whether interruptions get their own event stream."""

    strict_pre_declare: bool
    """Whether streams must be declared before they are used."""


class PlanSession:
    """Holds everything that outlives any one plan, and builds executors to run them."""

    permit: Permit
    """The durable permit. Every plan this session runs waits on it."""

    hooks: PlanHooks
    """Shared by reference with every plan, so setting one reaches a running plan."""

    dispatcher: Dispatcher
    """Subscribers outliving any one plan. Parent of every plan's own."""

    md: Metadata
    """Metadata outliving every plan. Holds the ``scan_id`` counter."""

    scan_id_source: Callable[[Metadata], SyncOrAsync[int]]
    """Computes the next ``scan_id`` from ``md``."""

    preprocessors: Sequence[Callable]
    """Applied to each plan as its executor is built, ``[f, g]`` as ``f(g(plan))``."""

    md_validator: Callable[[dict[str, Any]], None]
    md_normalizer: Callable[[dict[str, Any]], dict[str, Any]]
    run_bundler_cls: type[RunBundler]
    record_interruptions: bool
    strict_pre_declare: bool
    rewindable: bool
    """The default a plan starts with; the plan then owns its own."""

    def __init__(
        self,
        md: Metadata | None = None,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        log: LoggerAdapter | None = None,
        ignore_exceptions: bool = False,
    ) -> None:
        """A session on ``loop``, or on the loop running where this is constructed."""

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The event loop plans are executed on."""

    @property
    def suspenders(self) -> tuple[SuspenderBase, ...]:
        """The durable suspenders, those installed on this session."""

    @property
    def commands(self) -> dict[str, Callable]:
        """The ``Msg`` vocabulary the next plan will understand."""

    def make_executor(
        self,
        plan: Iterable[Msg],
        *,
        metadata: Metadata | None = None,
        subs: SubsLike | None = None,
    ) -> PlanExecutor:
        """Build an executor for ``plan`` from the settings as they stand now."""

    def install_suspender(self, suspender: SuspenderBase) -> None:
        """Install a suspender that holds up every plan this session runs."""

    def remove_suspender(self, suspender: SuspenderBase) -> None:
        """Uninstall a durable suspender and grant back what it was withholding."""

    def clear_suspenders(self) -> None:
        """Uninstall every durable suspender."""

    def register_command(self, name: str, func: Callable) -> None:
        """Add a ``Msg`` command to the vocabulary of every plan built after this."""

    def unregister_command(self, name: str) -> None:
        """Remove a command from that vocabulary."""


class PlanExecutor:
    """Executes one plan, and holds everything belonging to that plan alone."""

    run_start_uids: list[str]
    """The uid of every run this plan has opened."""

    exit_status: str
    """How the plan finished: success, abort, fail."""

    def __init__(
        self,
        plan: Iterable[Msg],
        env: PlanEnvironment,
        permit: Permit,
        hooks: PlanHooks,
        dispatcher: Dispatcher,
        *,
        identity: object | None = None,
        metadata: Metadata | None = None,
        subs: SubsLike | None = None,
        preprocessors: Sequence[Callable] = (),
        rewindable: bool = True,
        commands: Mapping[str, Callable] | None = None,
        without_commands: Collection[str] = (),
    ) -> None:
        """Build an executor for one plan. Usually `PlanSession.make_executor`."""

    async def run(self) -> Any:
        """Execute the plan and return what it returned. Once per executor."""

    @property
    def state(self) -> str:
        """Where this plan is: idle, running, pausing, paused, suspending, aborting..."""

    @property
    def resumable(self) -> bool:
        """Whether there is a checkpoint to rewind to."""

    @property
    def rewindable_flag(self) -> bool:
        """Whether messages may be replayed on a rewind. Plans change this constantly."""

    interrupted: bool
    """Whether this plan was interrupted before it finished."""

    @property
    def deferred_pause_requested(self) -> bool:
        """Whether a pause is pending, waiting for the next checkpoint."""

    def emit(self, name: str, doc: dict[str, Any]) -> None:
        """Send a document to this plan's subscribers, and to those outliving it."""

    def result(self, plan_return: Any) -> RunEngineResult:
        """Describe how the plan finished, given what it returned."""

    def block_run(self) -> None:
        """Hold `run` at its next resting point. Call on the loop."""

    def permit_run(self) -> None:
        """Let `run` proceed, undoing `block_run`. Call on the loop."""

    async def pause(self, defer: bool = False) -> None:
        """Bring the plan to rest, now or at the next checkpoint."""

    async def resume(self) -> None:
        """Let a paused plan continue from its last checkpoint."""

    async def abort(self, reason: str = "") -> None:
        """Stop a running or paused plan, marking its runs aborted."""

    async def stop(self) -> None:
        """Stop a running or paused plan, marking its runs successful."""

    async def halt(self) -> None:
        """Stop a running or paused plan without cleaning up after it."""
