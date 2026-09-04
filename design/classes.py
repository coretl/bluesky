"""Public external interface of the classes that make up the rewrite.

Still open, and deliberately not settled here:

1. ``Permit.wait_granted``.  Tom would drop it and leave ``wait_changed`` as the
   only primitive.  It is kept below because two call sites need it as a bare
   awaitable callable rather than a loop: the wait put in front of a plan whose
   permit is already withheld, and the future a suspension waits on.  Dropping
   it moves the same ``while not granted`` loop into both.
2. ``Msg('remove_suspender', ...)`` naming a *durable* suspender.  An executor
   no longer holds the session's suspenders, so the message can only cover the
   plan's own.  It either ignores a durable one or raises.
3. What a ``RunEngine`` is owed by an executor.  Seven members it drives are
   not listed below: ``interrupted``, ``emit``, ``result``, ``block_run``,
   ``permit_run``, ``deferred_pause_requested`` and ``dispatcher``.  Giving the
   plan's dispatcher a parent may account for the last two.
4. Whether ``identity`` is the right collapse of the old ``on_state_change``,
   or whether the ``RunEngine`` should set it rather than pass it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Collection, Hashable, Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from logging import LoggerAdapter
from typing import Any, Protocol, runtime_checkable

from bluesky.bundlers import RunBundler
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
    """Sends each document to the subscribers registered for it.

    Dispatchers chain, the same way permits do.  A plan's subscribers go in one
    of these with the session's as its ``parent``, so that a document reaches
    the subscribers outliving the plan before the ones that arrived with it --
    the order a single shared registry gave by construction -- and so that
    dropping the executor drops its subscriptions.  Chaining is the
    dispatcher's own business: nothing above it orders the two by hand.
    """

    def __init__(self, parent: Dispatcher | None = None, *, ignore_exceptions: bool = False) -> None:
        """A dispatcher whose documents also reach ``parent``'s subscribers."""

    def process(self, name: str, doc: dict[str, Any]) -> None:
        """Send ``doc`` to ``parent``'s subscribers, then to this one's."""

    def subscribe(self, func: Callable, name: str = "all") -> int:
        """Call ``func`` with every matching document. Returns a token."""

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
        """A permit on ``loop``, granted unless it or ``parent`` is withheld.

        A permit holds its parent and never a child, so a finished plan's
        permit is not kept alive by the session's.
        """

    @property
    def granted(self) -> bool:
        """Whether the plan may run: no reason here, and none above."""

    @property
    def reasons(self) -> dict[Hashable, Suspension]:
        """Every reason standing in the chain, keyed by whoever raised it.

        In the order they were raised, outermost permit first, because a
        suspension runs pre-plans in that order and post-plans in reverse.
        Reasons are kept apart rather than merged into one ``Suspension``: each
        condition runs its own pre-plan as it fires, so the supervisor needs
        them one by one.  Whoever wants one string joins the justifications.
        """

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
        """Wait until a reason is raised or dropped, anywhere in the chain.

        The only primitive.  A suspension already in progress waits on this to
        find out that another condition has joined it.
        """

    async def wait_granted(self) -> None:
        """Wait until no reason stands in the chain.

        ``while not self.granted: await self.wait_changed()``, kept because two
        call sites need it as a callable rather than a loop.  See (1) above.
        """


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
        """Subscribe to the signal, and withhold ``permit`` while it reads as bad.

        Subscribing is marshalled onto the permit's loop, which is where #1806
        puts it for a ``RunEngine``.  A suspender never learns what it is
        suspending; the permit is its only collaborator.

        Passing a ``RunEngine`` still works, with a ``DeprecationWarning``, and
        installs durably on its session as before.
        """

    def remove(self) -> None:
        """Unsubscribe from the signal, and grant back whatever it was withholding."""

    def __call__(self, value: Any, **kwargs: Any) -> None:
        """Ophyd's callback: withhold or grant, on whatever thread the signal uses."""

    @property
    def tripped(self) -> bool:
        """Whether the condition is bad right now."""


@dataclass
class PlanHooks:
    """Where a plan's progress can be watched from, live.

    Public on the session, and shared by reference with every plan it runs, so
    a ``RunEngine`` forwards to ``session.hooks.msg_hook`` and the rest rather
    than owning a property for each.
    """

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
    """Metadata for this plan, frozen as its executor is built.

    A snapshot, not the session's own mapping: a plan's environment does not
    change under it, so writing to ``session.md`` mid-plan takes effect on the
    next plan.  The session keeps whatever it was given -- a
    ``PersistentDict``, say -- and only its contents are copied here.
    """

    next_scan_id: Callable[[], SyncOrAsync[int]]
    """Allocates the ``scan_id`` for a run that is opening, and returns it.

    Reaches past the snapshot on purpose: the counter is durable, and two
    plans running at once must not be handed the same id.
    """

    md_validator: Callable[[dict[str, Any]], None]
    """Raises to stop a run starting."""

    md_normalizer: Callable[[dict[str, Any]], dict[str, Any]]
    """Returns the metadata a run will actually record."""

    run_bundler_cls: type[RunBundler]
    """Composes the documents for each open run."""

    run_engine_cls: type | None
    """What ``Msg('RE_class')`` reports; None means the executor answers for itself."""

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
    run_engine_cls: type | None
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
        """A session on ``loop``, or on the loop running where this is constructed.

        ``ignore_exceptions`` decides whether a raising subscriber interrupts
        data collection.  Set here and not afterwards: one dispatcher per plan
        means a settable one would have to be pushed to each in turn, and a
        plan's subscribers must not behave differently from the rest.
        """

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The event loop plans are executed on."""

    @property
    def suspenders(self) -> tuple[SuspenderBase, ...]:
        """The durable suspenders, those installed on this session."""

    @property
    def commands(self) -> dict[str, Callable]:
        """The ``Msg`` vocabulary the next plan will understand.

        Composed into each executor as it is built, so registering a command
        takes effect for the next plan and no ``Msg`` can change it from inside
        the plan it is running under.
        """

    def make_executor(
        self,
        plan: Iterable[Msg],
        *,
        metadata: Metadata | None = None,
        subs: SubsLike | None = None,
    ) -> PlanExecutor:
        """Build an executor for ``plan`` from the settings as they stand now.

        Gives it a ``Permit`` and a ``Dispatcher`` of its own, each a child of
        this session's, and a ``PlanEnvironment`` frozen here.  The caller owns
        what comes back; this session keeps no reference, so a headless caller
        can run more than one plan against one session.
        """

    def install_suspender(self, suspender: SuspenderBase) -> None:
        """Install a suspender that holds up every plan this session runs."""

    def remove_suspender(self, suspender: SuspenderBase) -> None:
        """Uninstall a durable suspender and grant back what it was withholding."""

    def clear_suspenders(self) -> None:
        """Uninstall every durable suspender."""

    def subscribe(self, func: Callable, name: str = "all") -> int:
        """Call ``func`` with every matching document, for as long as this session lives."""

    def unsubscribe(self, token: int) -> None:
        """Stop calling what ``subscribe`` returned this token for."""

    def register_command(self, name: str, func: Callable) -> None:
        """Add a ``Msg`` command to the vocabulary of every plan built after this."""

    def unregister_command(self, name: str) -> None:
        """Remove a command from that vocabulary."""


class PlanExecutor:
    """Executes one plan, and holds everything belonging to that plan alone."""

    run_start_uids: list[str]
    """The uid of every run this plan has opened."""

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
        """Build an executor for one plan. Usually `PlanSession.make_executor`.

        ``dispatcher`` is this plan's own, already holding the session's as its
        parent, so document order is settled there rather than here.

        ``identity`` is what a state change is logged as having happened to.
        A plan is executed by one of these, but what a user recognises in their
        logs is the long-lived ``RunEngine`` driving them.  Defaults to this
        executor.  The hook that *watches* state changes is ``hooks.state_hook``.

        If ``permit`` is already withheld, the executor waits for it before the
        plan's first message.  A suspension proper cannot do that job: there is
        no checkpoint yet, so it would abort the plan rather than hold it.
        """

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

    @property
    def suspenders(self) -> tuple[SuspenderBase, ...]:
        """The suspenders this plan installed for itself, which end with it.

        Not the session's: a plan is held up by those through the permit chain,
        and whoever wants both asks both.
        """

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
