"""The execution of a single plan, and the environment it runs in."""

import asyncio
import copy
import functools
import json
import typing
from collections import ChainMap, defaultdict, deque
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from dataclasses import dataclass
from enum import Enum
from logging import LoggerAdapter
from warnings import warn

from opentelemetry import trace
from opentelemetry.trace import Span

from bluesky._vendor.super_state_machine.errors import TransitionError
from bluesky._vendor.super_state_machine.extras import PropertyMachine
from bluesky._vendor.super_state_machine.machines import StateMachine

from ._loop import call_soon_or_now
from .bundlers import RunBundler, maybe_await
from .dispatcher import Dispatcher
from .log import logger, msg_logger, state_logger
from .protocols import (
    Flyable,
    Locatable,
    Movable,
    Pausable,
    Preparable,
    Readable,
    Stageable,
    Status,
    Stoppable,
    SyncOrAsync,
    Triggerable,
    check_supports,
)
from .suspenders import SuspenderBase
from .suspension import PlanLike, Suspension, SuspensionReason, join_justifications
from .tracing import tracer
from .utils import (
    AsyncInput,
    FailedPause,
    FailedStatus,
    IllegalMessageSequence,
    InvalidCommand,
    Msg,
    NoReplayAllowed,
    PlanHalt,
    RequestAbort,
    RequestStop,
    ensure_generator,
    normalize_subs_input,
    sanitize_np,
    single_gen,
    warn_if_msg_args_or_kwargs,
)

__all__ = [
    "NO_PLAN_RETURN",
    "UNCACHEABLE_COMMANDS",
    "Dispatcher",
    "LoggingPropertyMachine",
    "PlanRunner",
    "RunEngineMetadata",
    "RunEngineStateMachine",
    "WaitForTimeoutError",
    "default_scan_id_source",
]

# TODO: rename; the runner emits these without a RunEngine (#2054). Span names
# are queried and documented in docs/otel-tracing.rst, so a rename needs a
# deprecation of its own.
_SPAN_NAME_PREFIX = "Bluesky RunEngine"


class _NoPlanReturn:
    """The type of :data:`NO_PLAN_RETURN`.

    TODO: replace with a ``typing_extensions.Sentinel`` (PEP 661) once mypy
    narrows on it (#2016).
    """

    def __repr__(self) -> str:
        return "NO_PLAN_RETURN"


#: Returned in place of a plan's return value when the plan did not complete.
#: Distinct from ``None``.
NO_PLAN_RETURN = _NoPlanReturn()

#: Commands never replayed on a rewind: they act on the runner or are not
#: idempotent.
UNCACHEABLE_COMMANDS = frozenset(
    {
        "pause",
        "subscribe",
        "unsubscribe",
        "stage",
        "unstage",
        "monitor",
        "unmonitor",
        "open_run",
        "close_run",
        "install_suspender",
        "remove_suspender",
        "_start_suspender",
    }
)


class WaitForTimeoutError(TimeoutError): ...


class RunEngineStateMachine(StateMachine):
    """

    Attributes
    ----------
    is_idle
        State machine is in its idle state
    is_running
        State machine is in its running state
    is_paused
        State machine is paused.
    """

    class States(Enum):
        """state.name = state.value"""

        IDLE = "idle"

        RUNNING = "running"

        PAUSING = "pausing"
        PAUSED = "paused"

        HALTING = "halting"
        STOPPING = "stopping"
        ABORTING = "aborting"

        SUSPENDING = "suspending"

        PANICKED = "panicked"

        @classmethod
        def states(cls) -> list[str]:
            return [state.value for state in cls]

    class Meta:
        allow_empty = False
        initial_state = "idle"
        transitions = {
            # Notice that 'transitions' and 'named_transitions' have
            # opposite to <--> from structure.
            # from_state : [valid_to_states]
            "idle": ["running", "panicked"],
            "running": ["idle", "pausing", "halting", "stopping", "aborting", "suspending", "panicked"],
            "pausing": ["paused", "idle", "halting", "aborting", "panicked"],
            "suspending": ["running", "halting", "aborting", "panicked"],
            "paused": ["idle", "running", "halting", "stopping", "aborting", "panicked"],
            "halting": ["idle", "panicked"],
            "stopping": ["idle", "panicked"],
            "aborting": ["idle", "panicked"],
            "panicked": [],
        }
        named_checkers = [
            ("can_pause", "pausing"),
        ]


def announce_state_change(identity, hooks: "PlanHooks", old_value, value) -> None:
    """Log a state change against ``identity``, and call the state hook."""
    tags = {"old_state": old_value, "new_state": value, "RE": identity}

    state_logger.info("Change state on %r from %r -> %r", identity, old_value, value, extra=tags)
    hooks.state(value, old_value)


class LoggingPropertyMachine(PropertyMachine):
    """A state machine that calls its owner's ``_on_state_change(new, old)``, if not None."""

    def __init__(self, machine_type) -> None:
        super().__init__(machine_type)

    def __set__(self, obj, value) -> None:
        own = type(obj)
        old_value = self.__get__(obj, own)
        super().__set__(obj, value)
        value = self.__get__(obj, own)
        if getattr(obj, "_on_state_change", None) is not None:
            obj._on_state_change(value, old_value)


RunEngineMetadata = MutableMapping[str, typing.Any]


def default_scan_id_source(md: RunEngineMetadata) -> SyncOrAsync[int]:
    return md.get("scan_id", 0) + 1


def _default_event_loop() -> asyncio.AbstractEventLoop:
    """The running loop, or RuntimeError.

    Never the global loop from ``set_bluesky_event_loop``: a `RunEngine`
    passes its loop explicitly.
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        raise RuntimeError(
            "No event loop to run plans on. Either construct this from a "
            "coroutine, so that there is a running loop to adopt, or pass "
            "one in as loop=."
        ) from None


def _called(plan: PlanLike) -> typing.Iterable[Msg]:
    """Call ``plan`` if it is a generator function, else return it."""
    return plan() if callable(plan) else plan


def _default_md_validator(md: RunEngineMetadata) -> None:
    if "sample" in md and not (hasattr(md["sample"], "keys") or isinstance(md["sample"], str)):
        raise ValueError(
            "You specified 'sample' metadata. We give this field special "
            "significance in order to make your data easily searchable. "
            "Therefore, you must make 'sample' a string or a  "
            "dictionary, like so: "
            "GOOD: sample='dirt' "
            "GOOD: sample={'color': 'red', 'number': 5} "
            "BAD: sample=[1, 2] "
        )


def _default_md_normalizer(md: RunEngineMetadata) -> RunEngineMetadata:
    return md


@dataclass(frozen=True)
class PlanEnvironment:
    """Everything a plan needs to know about where it is being run.

    Frozen: a setting changed on the session takes effect for the next plan.

    Attributes
    ----------
    loop
        The event loop plans are executed on.
    log
        Where the runner logs to.
    md
        A copy of the session's metadata, taken as the runner was built.
    next_scan_id
        Returns the ``scan_id`` for a run that is opening, and stores it in
        the session's ``md``.
    md_validator
        Raises to prevent a run starting.
    md_normalizer
        Like ``md_validator``, but returns the normalized metadata.
    run_bundler_cls
        The bundler used to compose documents for each open run.
    record_interruptions
        Whether interruptions are recorded into their own event stream.
    strict_pre_declare
        Whether streams must be declared before they are used.
    """

    loop: asyncio.AbstractEventLoop
    log: LoggerAdapter
    md: RunEngineMetadata
    next_scan_id: Callable[[], SyncOrAsync[int]]
    md_validator: Callable
    md_normalizer: Callable
    run_bundler_cls: type[RunBundler]
    record_interruptions: bool
    strict_pre_declare: bool


def do_nothing(*args, **kwargs) -> None:
    """The unset hook: accepts anything and does nothing."""


@dataclass
class PlanHooks:
    """Callbacks observing a plan's progress.

    Unset hooks are `do_nothing`, and assigning ``None`` restores that.
    Shared by reference with every runner a session builds, so a change
    reaches a running plan.

    Attributes
    ----------
    msg
        Called with each `Msg` before it is processed.
    waiting
        Called with the status objects a plan is waiting on, and with ``None``
        once there are none. Drives progress bars.
    state
        Called ``f(new_state, old_state)`` on every state change.
    announce
        Called with a line for whoever is watching the plan.
    suspended
        Called with the reasons, keyed by who tripped them, as a suspension
        begins.
    proceed
        Awaited before the plan's first message and before it moves again
        after a pause. May be a coroutine. The only hook the runner waits on.
    held
        Called ``f(reasons, at_start)`` when the plan starts or resumes while
        tripped, and so waits for those reasons to clear.
    paused
        Called with no arguments when the plan comes to rest paused.
    """

    msg: Callable = do_nothing
    waiting: Callable = do_nothing
    state: Callable = do_nothing
    announce: Callable[[str], None] = do_nothing
    suspended: Callable[[typing.Mapping[typing.Hashable, SuspensionReason]], None] = do_nothing
    # Return value ignored.
    proceed: Callable[[], SyncOrAsync[typing.Any]] = do_nothing
    held: Callable[[typing.Mapping[typing.Hashable, SuspensionReason], bool], None] = do_nothing
    paused: Callable[[], None] = do_nothing

    def __setattr__(self, name: str, value) -> None:
        """Store ``None`` as `do_nothing`."""
        super().__setattr__(name, do_nothing if value is None else value)


class PlanRunner:
    """Executes one plan, which may contain any number of runs.

    .. warning::

       This API is provisional: names, signatures and behaviour may change
       without a deprecation period. `RunEngine`, which is built on it, is not
       provisional.

    Built for one plan, whose task starts at construction, and discarded
    after it. All of its state is used on the event loop only. Normally built
    by ``PlanSession.start``.

    Parameters
    ----------
    plan : iterable of Msg
        The plan to execute, or None for an idle runner.
    env : PlanEnvironment
        The loop, logger, metadata and run settings.
    suspension : Suspension
        What this plan waits on, normally a child of the session's.
    hooks : PlanHooks
        The session's hooks, shared by reference.
    dispatcher : Dispatcher
        Where this plan's documents and subscriptions go, normally a child of
        the session's.
    metadata : dict, optional
        Metadata for every run this plan opens.
    subs : callable, list, or dict, optional
        Subscriptions for this plan only.
    identity : object, optional
        What state changes are logged against and ``Msg('RE_class')`` reports
        the class of. Defaults to this runner.
    commands : mapping, optional
        Extra `Msg` commands, added to the built-ins.
    without_commands : collection of str, optional
        Built-in commands to leave out.
    preprocessors : sequence of callable, optional
        Applied to the plan at construction; ``[f, g]`` gives ``f(g(plan))``.
    initially_rewindable : bool, optional
        The initial value of :attr:`rewindable`.

    Attributes
    ----------
    run_start_uids : list
        The uid of every RunStart this plan has emitted, in order.
    interrupted : bool
        Whether the plan was paused, stopped, aborted or halted.
    exit_status : str
        ``'success'``, ``'abort'`` or ``'fail'``, recorded on the RunStop of
        every run still open.
    exit_reason : str
        The reason passed to :meth:`stop`, or the plan's exception as a string.
    exit_exception : BaseException or None
        The `~bluesky.utils.RequestStop`, `~bluesky.utils.RequestAbort` or
        `~bluesky.utils.PlanHalt` that ended a paused plan. None otherwise.
    """

    _state = LoggingPropertyMachine(RunEngineStateMachine)

    def __init__(
        self,
        plan,
        env: PlanEnvironment,
        suspension: Suspension,
        hooks: PlanHooks,
        dispatcher: "Dispatcher",
        *,
        metadata: dict | None = None,
        subs=None,
        identity: typing.Any = None,
        commands: typing.Mapping[str, Callable] | None = None,
        without_commands: typing.Collection[str] = (),
        preprocessors: typing.Sequence[Callable] = (),
        initially_rewindable: bool = True,
    ) -> None:
        self._env = env
        self._hooks = hooks
        self._identity = identity if identity is not None else self

        # Create the state machine entry now, before any other thread can read it.
        _ = self._state

        self._run_tracing_spans: list[Span] = []  # open tracing spans

        # Cleared to pause; run() waits until it is set.
        self._run_permit = asyncio.Event()
        self._run_permit.set()

        # Read by tests through RunEngine._run_bundlers.
        self._run_bundlers: dict[typing.Any, RunBundler] = {}  # open run -> bundler
        self._metadata_per_call: dict[typing.Any, typing.Any] = {}  # md for every run
        self._deferred_pause_requested: bool = False  # pause at next 'checkpoint'

        # An exception instance or class, to be thrown into the plan.
        self._exception: typing.Any = None
        # The outcome, read by whoever ran the plan. `_exception` is consumed by
        # the run loop, so `exit_exception` keeps it.
        self.interrupted: bool = False
        self.exit_status: str = "success"
        self.exit_reason: str = ""
        self.exit_exception: BaseException | None = None

        self._staged: set[typing.Any] = set()  # staged, not yet unstaged
        self._objs_seen: set[typing.Any] = set()  # every object seen in a Msg
        self._movable_objs_touched: set[typing.Any] = set()  # everything we 'set'
        self.run_start_uids: list[typing.Any] = []

        # Suspenders installed by this plan, removed when it ends.
        self._plan_suspenders: set[SuspenderBase] = set()

        self._suspension = suspension
        # True while `_holding` runs a wait for the suspension to clear.
        self._held = False
        # Watches the suspension; replaced on every start or resume.
        self._supervisor: asyncio.Task | None = None

        self._groups: defaultdict[str, set[Callable[[], asyncio.Future]]] = defaultdict(set)
        self._status_objs: defaultdict[typing.Any, set[typing.Any]] = defaultdict(set)
        # Read by test_flyer.py through RunEngine._seen_wait_and_move_on_keys.
        self._seen_wait_and_move_on_keys: set[typing.Any] = set()

        # Messages cached for a rewind; None once unrewindable (see `resumable`).
        self._msg_cache: deque[typing.Any] | None = deque()

        # Plans toggle this, so it lives on the runner, not the session.
        self._rewindable_flag: bool = initially_rewindable
        self._plan_stack: deque[typing.Any] = deque()  # generators to work off of
        self._response_stack: deque[typing.Any] = deque()  # responses to send into them

        # The task running the plan, created at the end of __init__. Tests
        # cancel it through RunEngine._task.
        self._task: asyncio.Task | None = None
        # Set once the plan is over, so its status objects stop reporting failures.
        self._pardon_failures = asyncio.Event()

        self._command_registry = self._build_command_registry(commands, without_commands)

        # Plan-scoped subscriptions, dropped with the runner.
        self._dispatcher = dispatcher
        for name, funcs in normalize_subs_input(subs).items():
            for func in funcs:
                self._dispatcher.subscribe(func, name)

        # Load the plan last, so preprocessors never see a half-built runner.
        self._plan = plan  # this ref is just used for metadata introspection
        if plan is None:
            # Idle runner: no plan and no task. A `RunEngine` keeps one between plans.
            return
        if metadata:
            self._metadata_per_call.update(metadata)
        gen = ensure_generator(plan)
        for wrapper_func in preprocessors:
            gen = wrapper_func(gen)
        self._push_plan(gen)

        # The plan cannot reach its first message before the loop next yields;
        # `PlanHooks.proceed` holds it there.
        self._task = asyncio.create_task(self._run())

    def _on_state_change(self, value, old_value) -> None:
        """Log a state change against the identity, and call the state hook."""
        announce_state_change(self._identity, self._hooks, old_value, value)

    async def _emit_async(self, name, doc) -> None:
        """Dispatch a document, on the loop."""
        self._dispatcher.process(name, doc)

    def _queue_emit(self, name, doc) -> None:
        """Dispatch a document on the loop, from any thread.

        For a monitor callback, which a sync ophyd signal calls on its own thread.
        """
        call_soon_or_now(self._loop, self._dispatcher.process, name, doc)

    @property
    def _loop(self) -> asyncio.AbstractEventLoop:
        """The event loop this plan is executed on."""
        return self._env.loop

    @property
    def suspenders(self) -> tuple[SuspenderBase, ...]:
        """The suspenders this plan installed, which end with it."""
        return tuple(self._plan_suspenders)

    def _drop_plan_suspender(self, suspender: SuspenderBase) -> None:
        """Uninstall a suspender this plan installed for itself."""
        self._plan_suspenders.discard(suspender)
        # `remove` clears the suspender's reason itself.
        suspender.remove()

    def clear_suspenders(self) -> None:
        """Uninstall every suspender this plan installed for itself."""
        for suspender in list(self._plan_suspenders):
            self._drop_plan_suspender(suspender)

    def _arrange_permission(self, at_start: bool) -> None:
        """Hold the plan while its suspension is tripped, and restart the supervisor.

        Called at the start of a plan and on a resume.
        """
        reasons = self._suspension.reasons
        tripped = bool(reasons)
        # No checkpoint yet to rewind to, so hold in band rather than suspend.
        # Not twice: a plan paused inside the hold waits in it again.
        if tripped and not self._held:
            self._push_plan(self._holding(self._hold()))
        if self._supervisor is not None:
            self._supervisor.cancel()
        self._supervisor = self._loop.create_task(self._supervise_suspension(tripped_at_start=tripped))
        # Last, so a hook that raises cannot skip the hold.
        if tripped:
            self._hooks.held(reasons, at_start)

    def _holding(
        self, plan: typing.Generator[Msg, typing.Any, typing.Any]
    ) -> typing.Generator[Msg, typing.Any, typing.Any]:
        """Run ``plan`` with `_held` set."""
        self._held = True
        try:
            return (yield from plan)
        finally:
            self._held = False

    def _hold(self) -> typing.Generator[Msg, typing.Any, None]:
        """Wait in band until the suspension clears."""
        while True:
            yield Msg("wait_for", None, [self._suspension.wait_cleared])
            # Woken by a pause and resumed still tripped: wait again.
            if not self._suspension.tripped:
                return

    @property
    def _run_task(self) -> asyncio.Task:
        """The task running the plan. RuntimeError for an idle runner."""
        if self._task is None:
            raise RuntimeError("No plan is running, so there is no task to interrupt.")
        return self._task

    async def _supervise_suspension(self, tripped_at_start=False) -> None:
        """Suspend the plan whenever its suspension is tripped.

        A reason tripping during a suspension joins it.
        """
        if tripped_at_start:
            # Held in band by `_arrange_permission`; wait for that to clear.
            await self._suspension.wait_cleared()
        while True:
            # Nothing while paused or pausing: `resume` restarts this task.
            while not (reasons := self._suspension.reasons) or self.state in ("paused", "pausing"):
                await self._suspension.wait_changed()
            opening = dict(reasons)
            if not self.resumable:
                # The plan is being torn down, so a suspension would never run.
                self._abort_unsuspendable()
                return
            self._hooks.suspended(opening)
            self._begin_suspension(opening)
            # The plan runs the suspension; wait for it to end.
            await self._suspension.wait_cleared()

    def _abort_unsuspendable(self) -> None:
        """End a plan that cannot be held, because it has no checkpoint."""
        self._hooks.announce("No checkpoint; cannot suspend.")
        self._hooks.announce("Aborting: running cleanup and marking exit_status as 'abort'...")
        self.interrupted = True
        self._exception = FailedPause()
        # Never paused: the supervisor skips a paused plan.
        self.state = "aborting"
        self._run_task.cancel()

    def _begin_suspension(self, opening: dict[typing.Hashable, SuspensionReason]) -> None:
        """Put a suspension for ``opening`` in front of the plan."""
        # The supervisor is cancelled before the runner goes idle.
        assert not self.state.is_idle, "a suspension reached a runner that had already finished"
        # The supervisor does nothing while paused.
        assert self.state != "paused", "a suspension was opened while the plan was paused"
        self._push_plan(single_gen(Msg("_start_suspender", None, opening)))
        self.state = "suspending"
        # Interrupt the run task so it reaches the pushed message.
        self._run_task.cancel()

    @property
    def suspension_reasons(self) -> typing.Mapping[typing.Hashable, SuspensionReason]:
        """What is holding this plan up, including the session's reasons, by who tripped it."""
        return self._suspension.reasons

    @property
    def resumable(self) -> bool:
        "i.e., can the plan in progress be rewound"
        return self._msg_cache is not None

    @property
    def rewindable(self) -> bool:
        """Whether messages may be replayed on a rewind. Plans change it."""
        return self._rewindable_flag

    @rewindable.setter
    def rewindable(self, value: bool) -> None:
        # A change drops the message cache. Both Msg('rewindable') and
        # RunEngine.rewindable come through here.
        cur_state = self._rewindable_flag
        self._rewindable_flag = bool(value)
        if self.resumable and self._rewindable_flag != cur_state:
            self._reset_checkpoint_state()

    @property
    def deferred_pause_requested(self) -> bool:
        """Whether a deferred pause is waiting for the next checkpoint."""
        return self._deferred_pause_requested

    def _push_plan(self, plan) -> None:
        """Push a plan, with no response for it yet."""
        self._plan_stack.append(plan)
        self._response_stack.append(None)

    @property
    def state(self):
        """This plan's state. One of {'idle', 'running', 'paused', ...}."""
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    async def _pause_objects(self) -> None:
        """Tell every `Pausable` object the plan has come to rest."""
        for obj in self._objs_seen:
            if isinstance(obj, Pausable):
                try:
                    await maybe_await(obj.pause())
                except NoReplayAllowed:
                    # The device cannot be replayed through, so drop the cache.
                    self._reset_checkpoint_state()

    async def _resume_objects(self) -> None:
        """Tell every `Pausable` object the plan is moving again.

        Only on the way back in: not on abort, stop or halt.
        """
        for obj in self._objs_seen:
            if isinstance(obj, Pausable):
                await maybe_await(obj.resume())

    async def _stop_movable_objects(self, *, success=True) -> None:
        "Call obj.stop() for all objects we have moved. Log any exceptions."
        for obj in self._movable_objs_touched:
            if isinstance(obj, Stoppable):
                try:
                    await maybe_await(obj.stop(success=success))
                except Exception:
                    self._env.log.exception("Failed to stop %r.", obj)
            else:
                self._env.log.debug("No 'stop' method available on %r", obj)

    def _destroy_open_run_tracing_spans(self) -> None:
        while len(self._run_tracing_spans):
            _span = self._run_tracing_spans.pop()
            _span.set_attribute("exit_status", "aborted")
            _span.end()

    def __await__(self) -> typing.Generator[typing.Any, None, typing.Any]:
        """Wait for the plan, and return what it returned.

        ::

            result = await runner

        Returns :data:`NO_PLAN_RETURN` if the plan did not complete. May be
        awaited more than once.
        """
        if self._task is None:
            raise RuntimeError(f"{self!r} was built with no plan, so there is nothing to wait for.")
        return self._task.__await__()

    def done(self) -> bool:
        """Whether the plan has finished, however it finished."""
        return self._task is not None and self._task.done()

    async def _run(self):
        """Run the plan; the task built in ``__init__``.

        Notes
        -----
        Pull messages from the plan, process them, send results back.

        Upon exit, clean up.
        - Call stop() on all objects that were 'set' or 'kickoff'.
        - Try to collect any uncollected flyers.
        - Try to unstage any devices left staged by the plan.
        - Try to remove any monitoring subscriptions left on by the plan.
        - If interrupting the middle of a run, try to emit a RunStop document.
        """
        # Before leaving 'idle', and outside the try: a cancel here ends a
        # plan that never ran, not one that aborted.
        await maybe_await(self._hooks.proceed())
        self._arrange_permission(at_start=True)
        stashed_exception = None
        debug = msg_logger.debug
        self.exit_status = "success"
        self.exit_reason = ""
        self.exit_exception = None
        # sentinel to decide if need to add to the response stack or not
        sentinel = object()
        plan_return = NO_PLAN_RETURN
        try:
            self.state = "running"
            while True:
                if self.state in ("pausing", "suspending"):
                    if not self.resumable:
                        self._run_permit.set()
                        stashed_exception = FailedPause()

                        self.state = "aborting"
                        continue
                # currently only using 'suspending' to get us into the
                # block above, we do not have a 'suspended' state
                # (yet)
                if self.state == "suspending":
                    self.state = "running"
                if not self._run_permit.is_set():
                    # A pause has been requested. First, put everything in a
                    # resting state.
                    assert self.state == "pausing"
                    # Remove any monitoring callbacks, but keep refs in
                    # self._monitor_params to re-instate them later.
                    for current_run in self._run_bundlers.values():
                        await current_run.suspend_monitors()
                    await self._stop_movable_objects(success=True)
                    await self._pause_objects()
                    self.state = "paused"
                    # Let RunEngine.__call__ return...
                    self._hooks.paused()

                    await self._run_permit.wait()
                    # See `PlanHooks.proceed`.
                    await maybe_await(self._hooks.proceed())
                    # Restore any monitors
                    for current_run in self._run_bundlers.values():
                        await current_run.restore_monitors()
                    if self.state == "paused":
                        # may be called by 'resume', 'stop', 'abort', 'halt'
                        self.state = "running"

                    # If we are here, we have come back to life either to
                    # continue (resume) or to clean up before exiting.

                assert len(self._response_stack) == len(self._plan_stack)
                # set resp to the sentinel so that if we fail in the sleep
                # we do not add an extra response
                resp = sentinel
                try:
                    # the new response to be added
                    new_response = None

                    # This 'await' must be here to ensure that this coroutine
                    # breaks out of its current behavior before trying to get
                    # the next message from the top of the generator stack in
                    # case there has been a pause requested.  Without this the
                    # next message after the pause may be processed first on
                    # resume (instead of the first message in self._msg_cache).
                    # This await also gives the co-routine for requesting
                    # suspends a chance to run.

                    # This sleep has to be inside of this try block so that any
                    # of the 'async' exceptions get thrown in the correct
                    # place.

                    # If we are handling an exception, then burn through the
                    # current plan stack before rather than allowing a pause or
                    # suspension to try and finish firing.
                    if stashed_exception is None:
                        await asyncio.sleep(0)
                    # always pop off a result, we are either sending it back in
                    # or throwing an exception in, in either case the left hand
                    # side of the yield in the plan will be moved past
                    resp = self._response_stack.pop()
                    # if any status tasks have failed, grab the exceptions.
                    # give priority to things pushed in from outside
                    if self._exception is not None:
                        stashed_exception = self._exception
                        self._exception = None
                    # The case where we have a stashed exception
                    if stashed_exception is not None or isinstance(resp, Exception):
                        # throw the exception at the current plan
                        try:
                            msg = self._plan_stack[-1].throw(stashed_exception or resp)
                        except Exception as e:
                            # The current plan did not handle it,
                            # maybe the next plan (if any) would like
                            # to try
                            self._plan_stack.pop()
                            # we have killed the current plan, do not give
                            # it a new response
                            resp = sentinel
                            # If there is at least one plan left in the stack,
                            # stash the new exception go back to top
                            if len(self._plan_stack):
                                stashed_exception = e
                                continue
                            # no plans left and still an unhandled exception
                            # re-raise to exit the infinite loop
                            else:
                                raise
                        # clear the stashed exception, the top plan
                        # handled it.
                        else:
                            stashed_exception = None
                    # The normal case of clean operation
                    else:
                        try:
                            msg = self._plan_stack[-1].send(resp)
                        # We have exhausted the top generator
                        except StopIteration:
                            # pop the dead generator go back to the top
                            self._plan_stack.pop()
                            # we have killed the current plan, do not give
                            # it a new response
                            resp = sentinel
                            if len(self._plan_stack):
                                continue
                            # or reraise to get out of the infinite loop
                            else:
                                raise
                        # Any other exception that comes out of the plan
                        except Exception as e:
                            # pop the dead plan, stash the exception and
                            # go to the top of the loop
                            self._plan_stack.pop()
                            # we have killed the current plan, do not give
                            # it a new response
                            resp = sentinel
                            if len(self._plan_stack):
                                stashed_exception = e
                                continue
                            # or reraise to get out of the infinite loop
                            else:
                                raise

                    # if we have a message hook, call it
                    self._hooks.msg(msg)
                    debug(
                        "%s(%r, *%r **%r, run=%r)",
                        msg.command,
                        msg.obj,
                        msg.args,
                        msg.kwargs,
                        msg.run,
                        extra={"msg_command": msg.command},
                    )

                    # update the running set of all objects we have seen
                    self._objs_seen.add(msg.obj)

                    # if this message can be cached for rewinding, cache it
                    if self._msg_cache is not None and self.rewindable and msg.command not in UNCACHEABLE_COMMANDS:
                        # We have a checkpoint.
                        self._msg_cache.append(msg)

                    # try to look up the coroutine to execute the command
                    if (
                        coro := self._command_registry.get(msg.command, key_absence_sentinel := object())
                    ) is key_absence_sentinel:
                        # flag invalid command
                        # and return to the top of the loop
                        new_response = InvalidCommand(msg.command)
                        continue

                    # try to finally run the command the user asked for
                    try:
                        # this is one of two places that 'async'
                        # exceptions (coming in via throw) can be
                        # raised
                        new_response = await coro(msg)

                    # special case `CancelledError` and let the outer
                    # exception block deal with it.
                    except asyncio.CancelledError:
                        raise
                    # any other exception, stash it and go to the top of loop
                    except Exception as e:
                        new_response = e
                        continue
                    # normal use, if it runs cleanly, stash the response and
                    # go to the top of the loop
                    else:
                        continue

                except KeyboardInterrupt:
                    # This only happens if some external code captures SIGINT
                    # -- overriding the RunEngine -- and then raises instead
                    # of (properly) calling the RunEngine's handler.
                    # See https://github.com/NSLS-II/bluesky/pull/242
                    self._env.log.warning(
                        "An unknown external library has improperly raised "
                        "KeyboardInterrupt. Intercepting and triggering a HALT."
                    )
                    await self.stop(success=False, finalize=False)
                except asyncio.CancelledError as e:
                    if self.state == "pausing":
                        # if we got a CancelledError and we are in the
                        # 'pausing' state clear the run suspension and
                        # bounce to the top
                        self._run_permit.clear()
                        continue
                    if self.state in ("halting", "stopping", "aborting"):
                        # if we got this while just keep going in tear-down
                        exception_map = {"halting": PlanHalt, "stopping": RequestStop, "aborting": RequestAbort}
                        # if the exception is not set bounce to the top
                        if stashed_exception is None:
                            stashed_exception = exception_map[self.state]
                        continue
                    if self.state == "suspending":
                        # just bounce to the top
                        continue
                    # if we are handling this twice, raise and leave the plans
                    # alone
                    if stashed_exception is e:
                        raise e
                    # the case where FailedPause, RequestAbort or a coro
                    # raised error is not already stashed in _exception
                    if stashed_exception is None:
                        stashed_exception = e
                finally:
                    # if we poped a response and did not pop a plan, we need
                    # to put the new response back on the stack
                    if resp is not sentinel:
                        self._response_stack.append(new_response)

        except StopIteration as e:
            self.exit_status = "success"
            plan_return = e.value
            # TODO Is the sleep here necessary?
            await asyncio.sleep(0)
        except RequestStop:
            self.exit_status = "success"
            # TODO Is the sleep here necessary?
            await asyncio.sleep(0)
        except (FailedPause, RequestAbort, asyncio.CancelledError, PlanHalt):
            self.exit_status = "abort"
            # TODO Is the sleep here necessary?
            await asyncio.sleep(0)
            self._env.log.exception("Run aborted")
        except GeneratorExit as err:
            self.exit_status = "fail"  # Exception raises during 'running'
            self.exit_reason = str(err)
            raise ValueError from err
        except Exception as err:
            self.exit_status = "fail"  # Exception raises during 'running'
            self.exit_reason = str(err)
            self._env.log.exception("Run aborted")
            raise err
        finally:
            # Some done_callbacks may still be alive in other threads.
            # Block them from creating new 'failed status' tasks on the loop.
            self._pardon_failures.set()
            # call stop() on every movable object we ever set()
            await self._stop_movable_objects(success=True)
            for current_run in self._run_bundlers.values():
                # Clear any uncleared monitoring callbacks.
                current_run.clear_monitors()
                # Try to collect any flyers that were kicked off but
                # not finished.  Some might not support partial
                # collection. We swallow errors.
                await current_run.backstop_collect()
            # in case we were interrupted between 'stage' and 'unstage'
            for obj in list(self._staged):
                try:
                    obj.unstage()
                except Exception:
                    self._env.log.exception("Failed to unstage %r.", obj)
                self._staged.remove(obj)

            # Emit RunStop if necessary.
            for key, current_run in self._run_bundlers.items():
                if current_run.run_is_open:
                    try:
                        await current_run.close_run(
                            Msg("close_run", exit_status=self.exit_status, reason=self.exit_reason, run_id=key)
                        )
                    except Exception:
                        self._env.log.error("Failed to close run %r.", current_run)
            self._run_bundlers.clear()

            for p in self._plan_stack:
                try:
                    p.close()
                except RuntimeError:
                    self._hooks.announce(f"The plan {p!r} tried to yield a value on close.  Please fix your plan.")

            self.clear_suspenders()
            if self._supervisor is not None:
                self._supervisor.cancel()

            self.state = "idle"

        self._env.log.info("Cleaned up from plan %r", self._plan)
        if isinstance(stashed_exception, asyncio.CancelledError):
            raise stashed_exception
        return plan_return

    def _close_run_trace(self, msg: Msg) -> None:
        exit_status = msg.kwargs.get("exit_status", self.exit_status)
        reason = msg.kwargs.get("reason", self.exit_reason)
        try:
            _span: Span = self._run_tracing_spans.pop()
            _span.set_attribute("exit_status", exit_status if exit_status is not None else "None")
            _span.set_attribute("reason", reason if reason is not None else "None")
            _span.end()
        except IndexError:
            logger.warning("No open traces left to close!")

    def _status_object_completed(
        self,
        ret,
        fut: asyncio.Future,
        pardon_failures: asyncio.Event,
        obj: typing.Any = None,
        action: str | None = None,
    ) -> None:
        """
        Task to run when a status object is finished. On the event loop.

        Parameters
        ----------
        ret : status object
        p_event : asyncio.Future
            held in the RunEngine's self._groups cache for waiting
        pardon_failuers : asyncio.Event
            tells us whether the __call__ this status object is over
        obj : object, optional
            the device the status object came from, for logging
        action : str, optional
            what the device was asked to do, for logging
        """
        self._env.log.debug("The object %r reports %r is done with status %r.", obj, action, ret.success)
        if not ret.success and not pardon_failures.is_set():
            # TODO: need a better channel to move this information back
            # to the run task.
            try:
                exc = ret.exception(timeout=0)
                raise FailedStatus(ret) from exc
            except Exception as e:
                self._exception = e
                fut.set_exception(e)
                # Retrieve it ourselves, to squash "Future exception was never
                # retrieved".
                fut.exception()
        else:
            fut.set_result(None)

    def _reset_checkpoint_state(self) -> None:
        """Forget the messages cached for a rewind, here and in every run."""
        if self._msg_cache is None:
            return

        self._msg_cache = deque()
        for current_run in self._run_bundlers.values():
            current_run.reset_checkpoint_state()

    def _add_status_to_group(self, obj: typing.Any, status_object: Status, group: str, action: str) -> None:
        loop = self._env.loop
        fut = loop.create_future()
        pardon_failures = self._pardon_failures
        settle = functools.partial(self._status_object_completed, status_object, fut, pardon_failures, obj, action)

        # A sync ophyd Status calls back on the thread that completed it, so
        # hand the work to the loop.
        def done_callback(*args: typing.Any, **kwargs: typing.Any) -> None:
            call_soon_or_now(loop, settle)

        try:
            status_object.add_callback(done_callback)
        except AttributeError:
            # for ophyd < v0.8.0
            status_object.finished_cb = done_callback  # type: ignore

        self._groups[group].add(lambda: fut)
        self._status_objs[group].add(status_object)

    def _rewind(self):
        """Clean up in preparation for resuming from a pause or suspension.

        Returns
        -------
        new_plan : generator
             A new plan made from the messages in the message cache

        """
        len_msg_cache = len(self._msg_cache)
        new_plan = ensure_generator(list(self._msg_cache))
        self._msg_cache = deque()
        if len_msg_cache:
            for current_run in self._run_bundlers.values():
                current_run.rewind()

        return new_plan

    async def pause(self, defer: bool = False) -> None:
        """Bring the plan to rest at a resting point. On the loop."""
        # We are pausing. Cancel any deferred pause previously requested.
        if not self.state.can_pause:
            raise TransitionError(f"Run Engine is in '{self.state}' state and can not be paused.")

        if defer:
            self._deferred_pause_requested = True
            self._hooks.announce("Deferred pause acknowledged. Continuing to checkpoint.")
            return

        self._hooks.announce("Pausing...")

        self._deferred_pause_requested = False
        self.interrupted = True
        self.state = "pausing"
        for current_run in self._run_bundlers.values():
            current_run.record_interruption("pause")

        self._run_task.cancel()

    async def resume(self) -> None:
        """Continue a paused plan from its last checkpoint. On the loop.

        Rewinds, tells devices, and releases the plan. If the suspension is
        tripped, the plan waits for it to clear, with no pre- or post-plans.
        """
        self.interrupted = False
        for current_run in self._run_bundlers.values():
            current_run.record_interruption("resume")
        self._push_plan(self._rewind())
        await self._resume_objects()
        # Ahead of the replayed messages.
        self._arrange_permission(at_start=False)
        # Last, so the plan wakes to all of the above.
        self._run_permit.set()

    async def stop(self, *, success: bool = True, finalize: bool = True, reason: str = "") -> None:
        """End the plan.

        =================  ===========  ============
        verb               ``success``  ``finalize``
        =================  ===========  ============
        `RunEngine.stop`   True         True
        `RunEngine.abort`  False        True
        `RunEngine.halt`   False        False
        =================  ===========  ============

        Parameters
        ----------
        success : bool, optional
            Whether the runs close as ``'success'`` rather than ``'abort'``.
        finalize : bool, optional
            Whether the plan may run its own cleanup. The runner's teardown
            always runs.
        reason : str, optional
            Recorded on the RunStop of every run still open.
        """
        if success and not finalize:
            raise RuntimeError(
                "success=True with finalize=False has no meaning: skipping the plan's "
                "own cleanup is how giving up on a plan differs from ending it, so a "
                "run cannot then be closed as a success. Use stop() to end the plan "
                "tidily, or halt() to give up on it."
            )
        if self.state.is_idle:
            raise TransitionError("RunEngine is already idle.")

        exception: type[BaseException]
        if success:
            verb, state, exception = "Stopping", "stopping", RequestStop
        elif finalize:
            verb, state, exception = "Aborting", "aborting", RequestAbort
        else:
            verb, state, exception = "Halting", "halting", PlanHalt
        cleanup = "running cleanup" if finalize else "skipping cleanup"
        exit_status = "success" if success else "abort"
        self._hooks.announce(f"{verb}: {cleanup} and marking exit_status as {exit_status!r}...")

        self.interrupted = True
        self.exit_reason = reason
        if not success:
            # Set here: a plan that catches the exception and returns would
            # otherwise end as a success.
            self.exit_status = "abort"
            # A stop closes its spans the ordinary way.
            self._destroy_open_run_tracing_spans()

        was_paused = self.state == "paused"
        self.state = state
        if was_paused:
            # A paused plan must be released to run its cleanup. Record the
            # exception first: once released, the run loop clears `_exception`.
            self.exit_exception = exception()
            self._exception = exception
            self._run_permit.set()
        else:
            self._run_task.cancel()

    async def _wait_for(self, msg: Msg) -> typing.Any:
        """Instruct the RunEngine to wait for futures and return the resulting tasks.

        Expected message object is:

            Msg('wait_for', None, awaitable_factories, **kwargs)

        The keyword arguments will be passed through to `asyncio.wait`.

        The callables in awaitable_factories must have the signature ::

           def fut_fac() -> awaitable:
               'This must work multiple times'

        """

        (futs,) = msg.args
        futs = [asyncio.ensure_future(f()) for f in futs]
        # Cancel on the way out, or they outlive the plan. Not on a timeout:
        # waiting on the same group again must find them still running.
        try:
            completed, pending = await asyncio.wait(futs, **msg.kwargs)
        except asyncio.CancelledError:
            for fut in futs:
                fut.cancel()
            raise
        if pending:
            raise WaitForTimeoutError("Plan failed to complete in the specified time")
        return futs

    def _bundler_for(self, run_key: typing.Any, complaint: str) -> RunBundler:
        """The bundler for ``run_key``'s open run; `IllegalMessageSequence` if none."""
        current_run = self._run_bundlers.get(run_key)
        if current_run is None:
            raise IllegalMessageSequence(complaint)
        return current_run

    async def _open_run(self, msg: Msg) -> typing.Any:
        """Instruct the RunEngine to start a new "run"

        Expected message object is:

            Msg('open_run', None, **kwargs)

        where **kwargs are any additional metadata that should go into
        the RunStart document
        """
        _span = tracer.start_span(f"{_SPAN_NAME_PREFIX} run")
        _set_span_msg_attributes(_span, msg)

        self._run_tracing_spans.append(_span)

        # TODO extract this from the Msg
        run_key = msg.run
        if run_key in self._run_bundlers:
            raise IllegalMessageSequence("A 'close_run' message was not received before the 'open_run' message")

        # Use the id returned: another runner may have written md since.
        scan_id = await maybe_await(self._env.next_scan_id())

        # For metadata below, info about plan passed to self.__call__ for.
        plan_type = type(self._plan).__name__
        plan_name = getattr(self._plan, "__name__", "")

        # Combine metadata, in order of decreasing precedence:
        md = ChainMap(
            self._metadata_per_call,  # from kwargs to self.__call__
            msg.kwargs,  # from 'open_run' Msg
            {
                "plan_type": plan_type,  # computed from self._plan
                "plan_name": plan_name,
                "scan_id": scan_id,  # from the session, for this run alone
            },
            self._env.md,
        )  # stateful, persistent metadata
        # The metadata is final. Validate it now, at the last moment.
        self._env.md_validator(dict(md))

        # Apply normalizer at the same level of the validator
        validated = self._env.md_normalizer(copy.deepcopy(md))

        current_run = self._run_bundlers[run_key] = self._env.run_bundler_cls(
            validated,
            self._env.record_interruptions,
            self._emit_async,
            self._queue_emit,
            self._env.log,
            strict_pre_declare=self._env.strict_pre_declare,
        )

        new_uid = await current_run.open_run(msg)
        self.run_start_uids.append(new_uid)
        return new_uid

    async def _close_run(self, msg: Msg) -> typing.Any:
        """Instruct the RunEngine to write the RunStop document

        Expected message object is:

            Msg('close_run', None, exit_status=None, reason=None)

        if *exit_stats* and *reason* are not provided, use the values
        stashed on the RE.
        """
        # TODO extract this from the Msg
        run_key = msg.run
        ims_msg = "A 'close_run' message was not received before the 'open_run' message"
        current_run = self._bundler_for(run_key, ims_msg)
        ret = await current_run.close_run(msg)
        del self._run_bundlers[run_key]
        self._close_run_trace(msg)
        return ret

    async def _create(self, msg: Msg) -> typing.Any:
        """Trigger the run engine to start bundling future obj.read() calls for
         an Event document

        Expected message object is:

            Msg('create', None, name='primary')
            Msg('create', name='primary')

        Note that the `name` kwarg will be the 'name' field of the resulting
        descriptor. So descriptor['name'] = msg.kwargs['name'].

        Also note that changing the 'name' of the Event will create a new
        Descriptor document.
        """
        run_key = msg.run
        ims_msg = "Cannot bundle readings without an open run. That is, 'create' must be preceded by 'open_run'."
        current_run = self._bundler_for(run_key, ims_msg)
        return await current_run.create(msg)

    async def _declare_stream(self, msg: Msg) -> typing.Any:
        """Trigger the run engine to start bundling future obj.describe() calls for
         an Event document

        Expected message object is:

            Msg('declare_stream', None, name='primary')
            Msg('declare_stream', name='primary')
            Msg('create', name='primary', collect=True)

        Note that the `name` kwarg will be the 'name' field of the resulting
        descriptor. So descriptor['name'] = msg.kwargs['name'].

        If `collect` is set to True (default false) then `describe_collect` will be called
        on declare_stream, rather than `describe`.
        """
        run_key = msg.run
        ims_msg = "Cannot bundle readings without an open run. That is, 'create' must be preceded by 'open_run'."
        current_run = self._bundler_for(run_key, ims_msg)
        return await current_run.declare_stream(msg)

    async def _read(self, msg: Msg) -> typing.Any:
        """
        Add a reading to the open event bundle.

        Expected message object is:

            Msg('read', obj)
        """
        obj = check_supports(msg.obj, Readable)
        # actually _read_ the object
        warn_if_msg_args_or_kwargs(msg, obj.read, msg.args, msg.kwargs)
        ret = await maybe_await(obj.read(*msg.args, **msg.kwargs))

        if ret is None:
            raise RuntimeError(
                f"The read of {obj.name} returned None. "
                "This is a bug in your object implementation, "
                "`read` must return a dictionary."
            )
        current_run = self._run_bundlers.get(msg.run)
        if current_run is not None:
            await current_run.read(msg, ret)

        return ret

    async def _locate(self, msg: Msg) -> typing.Any:
        """
        Locate some Movables and return their locations.

        Expected message object is:

            Msg('locate', obj1, ..., objn, squeeze=True)

        If a single obj is passed, obj.locate() is returned. If multiple objs
        are passed, obj.locate() is called in parallel for all objs and a list
        of the results returned. If squeeze is supplied and is False then it
        will always return a list of results even with a single object.
        """
        objs = [check_supports(obj, Locatable) for obj in (msg.obj,) + msg.args]
        # actually _locate_ the objects
        coros = [maybe_await(obj.locate()) for obj in objs]
        if len(coros) == 1 and msg.kwargs.get("squeeze", True):
            return await coros[0]
        else:
            return list(await asyncio.gather(*coros))

    async def _monitor(self, msg: Msg) -> typing.Any:
        """
        Monitor a signal. Emit event documents asynchronously.

        A descriptor document is emitted immediately. Then, a closure is
        defined that emits Event documents associated with that descriptor
        from a separate thread. This process is not related to the main
        bundling process (create/read/save).

        Expected message object is:

            Msg('monitor', obj, **kwargs)
            Msg('monitor', obj, name='event-stream-name', **kwargs)

        where kwargs are passed through to ``obj.subscribe()``
        """

        run_key = msg.run
        ims_msg = "A 'monitor' message was sent but no run is open."
        current_run = self._bundler_for(run_key, ims_msg)
        await current_run.monitor(msg)
        self._reset_checkpoint_state()

    async def _unmonitor(self, msg: Msg) -> typing.Any:
        """
        Stop monitoring; i.e., remove the callback emitting event documents.

        Expected message object is:

            Msg('unmonitor', obj)
        """
        run_key = msg.run
        ims_msg = "An 'unmonitor' message was sent but no run is open."
        current_run = self._bundler_for(run_key, ims_msg)
        await current_run.unmonitor(msg)
        self._reset_checkpoint_state()

    async def _save(self, msg: Msg) -> typing.Any:
        """Save the event that is currently being bundled

        Expected message object is:

            Msg('save')
        """
        run_key = msg.run
        # sanity check -- this should be caught by 'create' which makes this
        # code path impossible
        ims_msg = "A 'save' message was sent but no run is open."
        current_run = self._bundler_for(run_key, ims_msg)
        await current_run.save(msg)

    async def _drop(self, msg: Msg) -> typing.Any:
        """Drop the event that is currently being bundled

        Expected message object is:

            Msg('drop')
        """
        run_key = msg.run
        ims_msg = "A 'drop' message was sent but no run is open."
        current_run = self._bundler_for(run_key, ims_msg)
        await current_run.drop(msg)

    async def _prepare(self, msg: Msg) -> typing.Any:
        """Prepare a flyer for a flyscan

        Expected message object is:

        If `flyer_object` obeys the Preparable protocol, it should have a .prepare
        method that takes an argument to be set:

            Msg('prepare', flyer_object, value)

        Where value represents an initial state to move the flyer to.
        """
        obj = check_supports(msg.obj, Preparable)
        kwargs = dict(msg.kwargs)
        group = kwargs.pop("group", None)
        ret = obj.prepare(*msg.args, **kwargs)

        self._add_status_to_group(obj=obj, status_object=ret, group=group, action="prepare")

        return ret

    async def _kickoff(self, msg: Msg) -> typing.Any:
        """Start a flyscan object

        Special kwargs for the 'Msg' object in this function:
        group : str
            The blocking group to this flyer to

        Expected message object is:

        If `flyer_object` has a `kickoff` function that takes no arguments:

            Msg('kickoff', flyer_object)
            Msg('kickoff', flyer_object, group=<name>)

        If `flyer_object` has a `kickoff` function that takes
        `(start, stop, steps)` as its function arguments:

            Msg('kickoff', flyer_object, start, stop, step)
            Msg('kickoff', flyer_object, start, stop, step, group=<name>)
        """
        run_key = msg.run
        ims_msg = "A 'kickoff' message was sent but no run is open."
        current_run = self._bundler_for(run_key, ims_msg)

        _, obj, args, kwargs, _ = msg
        obj = check_supports(obj, Flyable)
        kwargs = dict(msg.kwargs)
        group = kwargs.pop("group", None)
        warn_if_msg_args_or_kwargs(msg, obj.kickoff, msg.args, kwargs)
        ret = obj.kickoff(*msg.args, **kwargs)
        await current_run.kickoff(msg)

        self._add_status_to_group(obj=obj, status_object=ret, group=group, action="kickoff")

        return ret

    @tracer.start_as_current_span(f"{_SPAN_NAME_PREFIX} complete")
    async def _complete(self, msg: Msg) -> typing.Any:
        """
        Tell a flyer, 'stop collecting, whenever you are ready'.

        The flyer returns a status object. Some flyers respond to this
        command by stopping collection and returning a finished status
        object immediately. Other flyers finish their given course and
        finish whenever they finish, irrespective of when this command is
        issued.

        Expected message object is:

            Msg('complete', flyer, group=<GROUP>)

        where <GROUP> is a hashable identifier.
        """
        _set_span_msg_attributes(trace.get_current_span(), msg)
        kwargs = dict(msg.kwargs)
        group = kwargs.pop("group", None)
        obj = check_supports(msg.obj, Flyable)
        warn_if_msg_args_or_kwargs(msg, obj.complete, msg.args, kwargs)
        ret = obj.complete(*msg.args, **kwargs)

        self._add_status_to_group(obj=obj, status_object=ret, group=group, action="complete")

        return ret

    @tracer.start_as_current_span(f"{_SPAN_NAME_PREFIX} collect")
    async def _collect(self, msg: Msg) -> typing.Any:
        """
        Collect data cached by a flyer and emit documents

        Expected message object is:

            Msg('collect', flyer_object)
            Msg('collect', flyer_object, stream=True, return_payload=False, name="a_name")
        """
        _set_span_msg_attributes(trace.get_current_span(), msg)
        run_key = msg.run
        # TODO add test exercising this path
        ims_msg = "A 'collect' message was sent but no run is open."
        current_run = self._bundler_for(run_key, ims_msg)
        return await current_run.collect(msg)

    async def _null(self, msg: Msg) -> typing.Any:
        """
        A no-op message, mainly for debugging and testing.
        """
        pass

    async def _RE_class(self, msg: Msg) -> typing.Any:
        """
        A no-op message, mainly for debugging and testing.
        """
        return type(self._identity)

    @tracer.start_as_current_span(f"{_SPAN_NAME_PREFIX} set")
    async def _set(self, msg: Msg) -> typing.Any:
        """
        Set a device and cache the returned status object.

        Also, note that the device has been touched so it can be stopped upon
        exit.

        Expected message object is

            Msg('set', obj, *args, **kwargs)

        where arguments are passed through to `obj.set(*args, **kwargs)`.
        """
        _set_span_msg_attributes(trace.get_current_span(), msg)
        obj = check_supports(msg.obj, Movable)
        kwargs = dict(msg.kwargs)
        group = kwargs.pop("group", None)
        self._movable_objs_touched.add(obj)
        ret = obj.set(*msg.args, **kwargs)

        self._add_status_to_group(obj=obj, status_object=ret, group=group, action="set")

        return ret

    async def _trigger(self, msg: Msg) -> typing.Any:
        """
        Trigger a device and cache the returned status object.

        Expected message object is:

            Msg('trigger', obj)
        """
        obj = check_supports(msg.obj, Triggerable)
        kwargs = dict(msg.kwargs)
        group = kwargs.pop("group", None)
        warn_if_msg_args_or_kwargs(msg, obj.trigger, msg.args, kwargs)
        ret = obj.trigger(*msg.args, **kwargs)

        self._add_status_to_group(obj=obj, status_object=ret, group=group, action="trigger")

        return ret

    @tracer.start_as_current_span(f"{_SPAN_NAME_PREFIX} wait")
    async def _wait(self, msg: Msg) -> bool:
        """Block progress until every object that was triggered or set
        with the keyword argument `group=<GROUP>` is done. Returns a boolean that is
        true when all triggered objects are done. When the keyword argument
        `error_on_timeout=<error_on_timeout>` is false, this method can return before all objects are done
        after a flush period given by the `timeout=<TIMEOUT>` keyword argument.

        Expected message object is:

            Msg('wait', group=<GROUP>, error_on_timeout=<ERROR_ON_TIMEOUT>)

        where ``<GROUP>`` is any hashable key and ``<ERROR_ON_TIMEOUT>`` is a boolean.
        """
        _set_span_msg_attributes(trace.get_current_span(), msg)
        done = False  # boolean that tracks whether waiting is complete
        if msg.args:
            (group,) = msg.args
        else:
            group = msg.kwargs["group"]
        error_on_timeout = msg.kwargs.get("error_on_timeout", True)
        watch = msg.kwargs.get("watch", ())
        watch_task: asyncio.Task | None = None
        if group:
            trace.get_current_span().set_attribute("group", group)
        else:
            trace.get_current_span().set_attribute("no_group_given", True)
        futs = self._groups.pop(group, set())
        if futs:
            status_objs = self._status_objs.pop(group)
            try:
                if not error_on_timeout:
                    if group not in self._seen_wait_and_move_on_keys:
                        self._seen_wait_and_move_on_keys.add(group)
                        self._hooks.waiting(status_objs)
                else:  # if error_on_timeout False
                    # Notify the waiting_hook function that the RunEngine is
                    # waiting for these status_objs to complete. Users can use
                    # the information these encapsulate to create a progress
                    # bar.
                    self._hooks.waiting(status_objs)

                async def wait_for_first_exception(futures: set) -> list[asyncio.Future]:
                    return await self._wait_for(
                        Msg(
                            "wait_for",
                            None,
                            futures,
                            return_when=asyncio.FIRST_EXCEPTION,
                            timeout=msg.kwargs.get("timeout", None),
                        )
                    )

                # Create the task waiting for the given group of statuses to complete
                # or one of them to fail
                status_task = asyncio.create_task(wait_for_first_exception(futs))
                if watch:
                    # Create a task that waits for an exception on any watch group
                    # so we know whether to stop the wait early because of a watcher failure
                    watch_futs = set()
                    for w in watch:
                        watch_futs.update(self._groups.get(w, set()))
                    watch_task = asyncio.create_task(wait_for_first_exception(watch_futs))

                    def cancel_status_task_if_error(fut: asyncio.Future[list[asyncio.Future]]) -> None:
                        # If _wait_for raised an exception, or if any of the status
                        # objects in the watch groups failed, cancel the status_task.
                        if fut.exception() or any(f.exception() for f in fut.result()):
                            status_task.cancel()

                    watch_task.add_done_callback(cancel_status_task_if_error)
                await status_task
            except WaitForTimeoutError:
                # We might wait to call wait again, so put the futures and status objects back in
                self._groups[group] = futs
                self._status_objs[group] = status_objs
                if error_on_timeout:
                    raise
            finally:
                if watch_task:
                    watch_task.cancel()
                if error_on_timeout:
                    # Notify the waiting_hook function that we have moved on by
                    # sending it `None`. If all goes well, it could have
                    # inferred this from the status_obj, but there are edge
                    # cases.
                    self._hooks.waiting(None)
                    done = True
                else:
                    done = all(obj.done for obj in status_objs)
                    if done:
                        self._hooks.waiting(None)
                        self._seen_wait_and_move_on_keys.remove(group)
        else:
            done = True
        return done

    async def _sleep(self, msg: Msg) -> typing.Any:
        """
        Sleep the event loop.

        Expected message object is:

            Msg('sleep', None, sleep_time)

        where `sleep_time` is in seconds
        """
        await asyncio.sleep(*msg.args)

    async def _pause(self, msg: Msg) -> typing.Any:
        """Request the run engine to pause

        Expected message object is:

            Msg('pause', defer=False, name=None, callback=None)

        See RunEngine.request_pause() docstring for explanation of the three
        keyword arguments in the `Msg` signature
        """
        await self.pause(*msg.args, **msg.kwargs)

    async def _resume_from_suspender(self, msg: Msg) -> typing.Any:
        """The suspension is over: tell the devices. Msg('_resume_from_suspender')"""
        await self._resume_objects()

    async def _checkpoint(self, msg: Msg) -> typing.Any:
        """Instruct the RunEngine to create a checkpoint so that we can rewind
        to this point if necessary

        Expected message object is:

            Msg('checkpoint')
        """
        for current_run in self._run_bundlers.values():
            if current_run.bundling:
                raise IllegalMessageSequence("Cannot 'checkpoint' after 'create' and before 'save'. Aborting!")

        self._reset_checkpoint_state()

        if self._deferred_pause_requested:
            # We are at a checkpoint; we are done deferring the pause.
            # Give the _check_for_signals coroutine time to look for
            # additional SIGINTs that would trigger an abort.
            await asyncio.sleep(0.5)
            await self.pause(defer=False)

    async def _clear_checkpoint(self, msg: Msg) -> typing.Any:
        """Clear a set checkpoint

        Expected message object is:

            Msg('clear_checkpoint')
        """
        # clear message cache
        self._msg_cache = None
        # clear stashed
        for current_run in self._run_bundlers.values():
            await current_run.clear_checkpoint(msg)

    async def _rewindable(self, msg: Msg) -> typing.Any:
        """Set rewindable state of RunEngine

        Expected message object is:

            Msg('rewindable', None, bool or None)
        """

        (rw_flag,) = msg.args
        if rw_flag is not None:
            self.rewindable = rw_flag

        return self.rewindable

    async def _configure(self, msg: Msg) -> typing.Any:
        """Configure an object

        Expected message object is:

            Msg('configure', object, *args, **kwargs)

        which results in this call:

            object.configure(*args, **kwargs)
        """
        current_run = self._run_bundlers.get(msg.run)
        if current_run is not None and current_run.bundling:
            ims_msg = "Cannot configure after 'create' but before 'save' Aborting!"
            raise IllegalMessageSequence(ims_msg)
        _, obj, args, kwargs, _ = msg

        old, new = obj.configure(*args, **kwargs)
        if current_run:
            await current_run.configure(msg)
        return old, new

    async def _stage(self, msg: Msg) -> typing.Any:
        """Instruct the RunEngine to stage the object

        Expected message object is:

            Msg('stage', object)
        """
        _, obj, args, kwargs, _ = msg
        # If an object has no 'stage' method, assume there is nothing to do.
        if not isinstance(obj, Stageable):
            return []
        group = kwargs.pop("group", None)
        ret = obj.stage()
        self._staged.add(obj)  # add first in case of failure below
        self._reset_checkpoint_state()

        if not isinstance(ret, Status):
            return ret

        self._add_status_to_group(obj=obj, status_object=ret, group=group, action="stage")

        return ret

    async def _unstage(self, msg: Msg) -> typing.Any:
        """Instruct the RunEngine to unstage the object

        Expected message object is:

            Msg('unstage', object)
        """
        _, obj, args, kwargs, _ = msg
        # If an object has no 'unstage' method, assume there is nothing to do.
        if not isinstance(obj, Stageable):
            return []
        group = kwargs.pop("group", None)
        ret = obj.unstage()
        # use `discard()` to ignore objects that are not in the staged set.
        self._staged.discard(obj)
        self._reset_checkpoint_state()

        if not isinstance(ret, Status):
            return ret

        self._add_status_to_group(obj=obj, status_object=ret, group=group, action="unstage")

        return ret

    async def _stop(self, msg: Msg) -> typing.Any:
        """
        Stop a device.

        Expected message object is:

            Msg('stop', obj)
        """
        obj = check_supports(msg.obj, Stoppable)
        return await maybe_await(obj.stop())  # nominally, this returns None

    async def _subscribe(self, msg: Msg) -> typing.Any:
        """
        Add a subscription after the run has started.

        This, like subscriptions passed to __call__, will be removed at the
        end by the RunEngine.

        Expected message object is:

            Msg('subscribe', None, callback_function, document_name)

        where `document_name` is one of:

            {'start', 'descriptor', 'event', 'stop', 'all'}

        and `callback_function` is expected to have a signature of:

            ``f(name, document)``

            where name is one of the ``document_name`` options and ``document``
            is one of the document dictionaries in the event model.

        See the docstring of bluesky.run_engine.Dispatcher.subscribe() for more
        information.
        """
        self._env.log.debug("Adding subscription %r", msg)
        _, obj, args, kwargs, _ = msg
        token = self._dispatcher.subscribe(*args, **kwargs)
        self._reset_checkpoint_state()
        return token

    async def _unsubscribe(self, msg: Msg) -> typing.Any:
        """
        Remove a subscription during a call -- useful for a multi-run call
        where subscriptions are wanted for some runs but not others.

        Expected message object is:

            Msg('unsubscribe', None, TOKEN)
            Msg('unsubscribe', token=TOKEN)

        where ``TOKEN`` is the return value from ``RunEngine._subscribe()``
        """
        self._env.log.debug("Removing subscription %r", msg)
        _, obj, arg, kwargs, _ = msg
        if (token := kwargs.get("token", key_absence_sentinel := object())) is key_absence_sentinel:
            (token,) = arg
        self._dispatcher.unsubscribe(token)
        self._reset_checkpoint_state()

    async def _input(self, msg: Msg) -> typing.Any:
        """
        Process a 'input' Msg. Expected Msg:

            Msg('input', None)
            Msg('input', None, prompt='>')  # customize prompt
        """
        prompt = msg.kwargs.get("prompt", "")
        async_input = AsyncInput(self._env.loop)
        ask = functools.partial(async_input, end="", flush=True)
        return await ask(prompt)

    async def _install_suspender(self, msg: Msg) -> typing.Any:
        """Install a suspender for this plan only. Msg('install_suspender', None, suspender)"""
        suspender = msg.args[0]
        self._plan_suspenders.add(suspender)
        suspender.install(self._suspension)

    async def _remove_suspender(self, msg: Msg) -> typing.Any:
        """Remove a suspender this plan installed. Msg('remove_suspender', None, suspender)"""
        suspender = msg.args[0]
        if suspender not in self._plan_suspenders:
            warn(
                f"{suspender!r} is not installed on this plan, so "
                "Msg('remove_suspender') ignored it. A plan can only remove a "
                "suspender it installed itself.",
                stacklevel=2,
            )
            return
        self._drop_plan_suspender(suspender)

    async def _start_suspender(self, msg: Msg) -> typing.Any:
        """
        An internal message to do the initial work of starting a suspender
        """
        (opening,) = msg.args
        for current_run in self._run_bundlers.values():
            current_run.record_interruption(join_justifications(opening) or "suspended")
        await self._stop_movable_objects(success=True)
        await self._pause_objects()
        rewind_plan = self._rewind()
        was_rewindable = self.rewindable
        # A copy, so reasons that join later do not change it.
        opening_order = list(opening.values())
        seen = set(opening)
        # Reasons that join, for the post-plans.
        joined: list[SuspensionReason] = []

        async def a_change() -> None:
            """Park until the suspension clears, or a reason outside ``seen`` joins."""
            while self._suspension.tripped and seen.issuperset(self._suspension.reasons):
                await self._suspension.wait_changed()

        def until_released() -> typing.Generator[Msg, typing.Any, None]:
            # None of this is replayed: rewinding is what happens after it.
            yield Msg("rewindable", None, False)
            # The pre-plans of the reasons that opened the suspension.
            for reason in opening_order:
                if reason.pre_plan is not None:
                    yield from ensure_generator(_called(reason.pre_plan))
            # Hold until the suspension clears. A reason tripping meanwhile
            # joins, and its pre-plan runs here.
            while True:
                yield Msg("wait_for", None, [a_change])
                # A pause wakes this too.
                if not self._suspension.tripped:
                    break
                joining = {key: reason for key, reason in self._suspension.reasons.items() if key not in seen}
                for key, reason in joining.items():
                    seen.add(key)
                    joined.append(reason)
                    if reason.pre_plan is not None:
                        yield from ensure_generator(_called(reason.pre_plan))

        def suspension() -> typing.Generator[Msg, typing.Any, None]:
            yield from self._holding(until_released())
            yield Msg("_resume_from_suspender", None)
            # Post-plans for every reason that joined too, in reverse.
            for reason in [*reversed(joined), *reversed(opening_order)]:
                if reason.post_plan is not None:
                    yield from ensure_generator(_called(reason.post_plan))
            yield Msg("rewindable", None, was_rewindable)
            yield from rewind_plan

        self._push_plan(suspension())

    # The built-in commands, as unbound methods, so `PlanSession.commands`
    # can read them without a runner.
    _DEFAULT_COMMANDS: typing.ClassVar[dict[str, Callable[["PlanRunner", Msg], Awaitable[typing.Any]]]] = {
        "declare_stream": _declare_stream,
        "create": _create,
        "save": _save,
        "drop": _drop,
        "read": _read,
        "locate": _locate,
        "monitor": _monitor,
        "unmonitor": _unmonitor,
        "null": _null,
        "RE_class": _RE_class,
        "stop": _stop,
        "set": _set,
        "trigger": _trigger,
        "sleep": _sleep,
        "wait": _wait,
        "checkpoint": _checkpoint,
        "clear_checkpoint": _clear_checkpoint,
        "rewindable": _rewindable,
        "pause": _pause,
        "_resume_from_suspender": _resume_from_suspender,
        "_start_suspender": _start_suspender,
        "prepare": _prepare,
        "collect": _collect,
        "kickoff": _kickoff,
        "complete": _complete,
        "configure": _configure,
        "stage": _stage,
        "unstage": _unstage,
        "subscribe": _subscribe,
        "unsubscribe": _unsubscribe,
        "open_run": _open_run,
        "close_run": _close_run,
        "wait_for": _wait_for,
        "input": _input,
        "install_suspender": _install_suspender,
        "remove_suspender": _remove_suspender,
    }

    def _build_command_registry(
        self,
        commands: Mapping[str, Callable] | None,
        without_commands: typing.Collection[str],
    ) -> dict[str, Callable[[Msg], Awaitable[typing.Any]]]:
        """The commands this plan understands: built-ins, plus ``commands``, less ``without_commands``."""
        registry: dict[str, Callable[[Msg], Awaitable[typing.Any]]] = {
            name: fn.__get__(self) for name, fn in self._DEFAULT_COMMANDS.items()
        }
        registry.update(commands or {})
        for name in without_commands:
            registry.pop(name, None)
        return registry


def _set_span_msg_attributes(span: Span, msg: Msg) -> None:
    span.set_attribute("msg.command", msg.command)
    span.set_attribute("msg.args", sanitize_np(msg.args))
    span.set_attribute("msg.kwargs", json.dumps(msg.kwargs, default=repr))
    span.set_attribute("msg.obj", repr(msg.obj)) if msg.obj else span.set_attribute("msg.no_obj_given", True)
