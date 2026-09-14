"""The environment plans run in, and the execution of a single plan.

See :class:`PlanSession` and :class:`PlanExecutor`. A `bluesky.run_engine.RunEngine`
composes the two and drives them from a terminal on the main thread.
"""

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
from .suspensions import Suspension, SuspensionReason, join_justifications
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
    "PlanExecutor",
    "RunEngineMetadata",
    "RunEngineStateMachine",
    "WaitForTimeoutError",
    "default_scan_id_source",
]

# TODO: rename this; tracked by #2054. Inaccurate since the split, because
# these spans are emitted by the executor, which runs plans with no RunEngine
# in the process. Held anyway, because span names are the query surface --
# renaming would silently stop every saved query and dashboard built on the old
# ones from matching -- and because they are documented in
# docs/otel-tracing.rst. The rename needs a deprecation story of its own, which
# is what #2054 is for.
_SPAN_NAME_PREFIX = "Bluesky RunEngine"


class _NoPlanReturn:
    """The type of :data:`NO_PLAN_RETURN`.

    TODO: this class exists only to give the singleton below a type and a
    repr. Replace both with a ``typing_extensions.Sentinel`` (PEP 661), which
    ships today but which mypy does not yet narrow on; when it does, the
    sentinel needs no companion class and this one goes away. Tracked by #2016,
    which covers the rest of bluesky's sentinels too.
    """

    def __repr__(self) -> str:
        return "NO_PLAN_RETURN"


#: Returned in place of a plan's return value when the plan did not run to
#: completion, and so never returned one. Distinguishable from a plan that
#: completed and returned ``None``.
NO_PLAN_RETURN = _NoPlanReturn()

#: Commands that must not be replayed when rewinding to a checkpoint, either
#: because they act on the RunEngine itself or because they are not idempotent.
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
        def states(cls):
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
    """Log a state change, and tell the state hook about it.

    ``identity`` is what the log record names as having changed state.
    """
    tags = {"old_state": old_value, "new_state": value, "RE": identity}

    state_logger.info("Change state on %r from %r -> %r", identity, old_value, value, extra=tags)
    hooks.state(value, old_value)


class LoggingPropertyMachine(PropertyMachine):
    """A state machine that announces every transition.

    Expects the owning object to have an ``_on_state_change`` attribute that is
    ``None``, or a callable with signature ``f(new_value, old_value)``. See
    :func:`announce_state_change`.
    """

    def __init__(self, machine_type):
        super().__init__(machine_type)

    def __set__(self, obj, value):
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
    """The loop to use when none was given: the one running right here.

    Deliberately does not fall back on the process-wide loop a `RunEngine`
    registers with ``set_bluesky_event_loop``. That global belongs to the
    prompt -- it is what ``autoawait`` and `call_in_bluesky_event_loop` are
    for -- and a session reaching for it would quietly bind a headless plan to
    whichever RunEngine happened to be constructed first. A `RunEngine` always
    passes its loop in explicitly, so nothing that had one loses it.
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        raise RuntimeError(
            "No event loop to run plans on. Either construct this from a "
            "coroutine, so that there is a running loop to adopt, or pass "
            "one in as loop=."
        ) from None


def _called(plan):
    """A pre- or post-plan may be given as a generator function or an iterable."""
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

    Frozen: an executor is built for one plan, and what it was told as it was
    built is what that plan sees to the end. Changing a setting on the
    `PlanSession` therefore takes effect for the *next* plan, not the running
    one. Everything here is read by the executor while its plan runs.

    Attributes
    ----------
    loop
        The event loop plans are executed on.
    log
        Where the executor logs to.
    md
        The metadata this plan runs under, snapshotted as its executor was
        built. A plain ``dict``, copied from whatever the session holds -- a
        `bluesky.utils.PersistentDict` is a supported choice there, and only
        its contents are copied, never the store itself.
    next_scan_id
        Called by the executor as each run opens, and returns the ``scan_id``
        for that run. Reaches past the snapshot on purpose: the counter is
        durable, so the session computes it, stores it in its own ``md`` as the
        starting point for the next one, and returns it; concurrent callers are
        serialised so that no two runs are given the same id.
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
    """What an unset hook is. Accepts whatever its hook is called with."""


@dataclass
class PlanHooks:
    """The places a plan's progress can be observed from.

    Every hook is callable, and an unset one is `do_nothing` rather than
    ``None``; assigning ``None`` is how a caller says nobody is listening.

    Mutable, and shared by reference with every executor a session builds --
    the opposite guarantee to `PlanEnvironment` -- so setting ``RE.msg_hook``
    while a plan is running takes effect on *that* plan.

    Attributes
    ----------
    msg
        Called with each `Msg` before it is processed. ``RE.msg_hook = print``
        is the usual debugging idiom.
    waiting
        Called with the status objects a plan is waiting on, and with ``None``
        once there is nothing left to wait for. Drives progress bars.
    state
        Called ``f(new_state, old_state)`` on every state change.
    announce
        Called with a line addressed to whoever is watching the plan. Says what
        happened, never what to press: a `RunEngine` prints it, a service may
        put it anywhere.
    suspend
        Called with the reasons standing as a suspension begins, keyed by
        whoever raised them. The reasons rather than a joined string, because
        a caller that is handed prose can only print it: one that is handed
        the mapping can count it, pick a reason out of it, or join it the way
        `RunEngine` does. Separate from `announce` because how a user
        interrupts a suspended plan depends on what is driving it, so the
        wording is the caller's.
    pause
        Called with no arguments when an executor comes to rest paused. A
        `RunEngine` uses this to release the main thread; a headless caller has
        no thread to release and can leave it unset.
    """

    msg: Callable = do_nothing
    waiting: Callable = do_nothing
    state: Callable = do_nothing
    announce: Callable[[str], None] = do_nothing
    suspend: Callable[[typing.Mapping[typing.Hashable, SuspensionReason]], None] = do_nothing
    pause: Callable[[], None] = do_nothing

    def __setattr__(self, name: str, value) -> None:
        """``None`` means "nobody is listening", and is stored as `do_nothing`.

        Taken here rather than at each place that assigns one, so that setting a
        hook to ``None`` -- which is how a user turns one off, and what
        `bluesky.magics` does around every ``%mov`` -- cannot leave something
        here that the next report would fall over.
        """
        super().__setattr__(name, do_nothing if value is None else value)


class PlanExecutor:
    """Executes one plan, which may contain any number of runs.

    An executor owns everything belonging to one plan's execution: the stack of
    generators being worked off, the messages cached for rewinding, the devices
    seen and staged, and the status objects being waited on. It is built for a
    plan and discarded after it.

    Every method that touches its state runs on its session's event loop, and it
    holds no locks and no threading primitives. :meth:`emit` is the one
    exception -- a sync ophyd signal fires its monitor callback on the device's
    own thread and reaches ``emit`` directly, so a document can be given to
    subscribers off the loop. See its docstring.

    Prefer :meth:`PlanSession.make_executor` to constructing one directly: a
    session must know which executor is running, because that is how a
    suspender reaches the plan in progress.

    Parameters
    ----------
    plan : iterable of Msg
        The plan to execute. Taken here rather than by :meth:`run` so that an
        executor is structurally for one plan: there is no second plan to hand
        it. ``env``'s preprocessors are applied to it now, so a malformed plan
        raises on the calling thread rather than inside the task.
    env : PlanEnvironment
        Where the plan is being run: the loop, the logger, the metadata and the
        settings that compose each run.
    suspension : Suspension
        The suspension this plan waits on. Normally built with the session's
        durable suspension as its parent, so that a suspender installed on the
        session holds up every plan it is running.
    hooks : PlanHooks
        The observation points. Shared by reference with the session rather
        than copied, so setting one mid-plan reaches the plan already running.
    dispatcher : Dispatcher
        Where this plan's documents go, and where subscriptions made for this
        plan live. Normally built with the session's as its parent, so that
        subscribers outliving the plan see a document first -- the order a
        single shared registry gave by construction.
    metadata : dict, optional
        Metadata for every run this plan opens.
    subs : callable, list, or dict, optional
        Subscriptions lasting only as long as this plan.
    identity : object, optional
        What a state change is logged as having happened to, and what
        ``Msg('RE_class')`` reports the class of. A plan is executed by one of
        these, but what a user recognises in their logs is the long-lived
        `RunEngine` driving them, so whoever is driving names itself. Defaults
        to this executor, which is what a headless caller wants. The hook that
        *watches* state changes is ``hooks.state``.
    commands : mapping, optional
        Extra `Msg` commands, composed over the built-ins. A session passes the
        ones a user registered, plus ``install_suspender`` and
        ``remove_suspender``, which only mean anything when there is a session
        to install into.
    without_commands : collection of str, optional
        Built-in commands to leave out.
    preprocessors : sequence of callable, optional
        Generator functions applied to the plan now, composed in order, so
        that ``[f, g]`` is applied as ``f(g(plan))``. Consumed here rather
        than kept: once the plan is wrapped there is nothing left to preprocess
        which is why this is an argument and not part of `PlanEnvironment`.
    initially_rewindable : bool, optional
        What :attr:`rewindable` starts at. The plan then owns it and changes it
        constantly -- see `bluesky.preprocessors` -- so this is an argument
        rather than part of the environment: the session's default is read once,
        at construction, and the live value lives on this executor.
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
    ):
        self._env = env
        self._hooks = hooks
        self._identity = identity if identity is not None else self

        # Set when the plan comes to rest paused; see the pause block in `run`.
        self._cleared_when_paused = True
        # The task watching this plan's suspension, replaced whenever the plan
        # enters from a state where the user had control. None until `run`.
        self._supervisor: asyncio.Task | None = None
        # When cleared, run() will pause until it is set again.
        self._run_permit = asyncio.Event()
        self._run_permit.set()
        # When set, done callbacks from status objects belonging to this plan
        # stop reporting failures: the plan they belong to is over.
        self._pardon_failures = asyncio.Event()
        # run() may only be entered once. A finished executor is 'idle' again,
        # so its state cannot tell a spent one from a fresh one.
        self._spent = False

        # Reached for by name through RunEngine._task, which bluesky's own
        # tests use to cancel a plan (test_run_engine.py). Private because
        # cancelling the task behind a plan's back is not a supported way to
        # stop one -- halt() is -- and the forward exists so those call sites
        # keep working, not because they are a good idea.
        self._task: asyncio.Task | None = None  # the task running this plan
        self._plan_stack: deque[typing.Any] = deque()  # generators to work off of
        self._response_stack: deque[typing.Any] = deque()  # responses to send into them
        # Processed msgs, for rewinding. None once a 'clear_checkpoint' has
        # made this plan unrewindable, which is what `resumable` tests for.
        self._msg_cache: deque[typing.Any] | None = deque()

        # Whether messages may be replayed on a rewind. Seeded from the
        # session's default, then owned outright: plans toggle this constantly
        # -- every trigger_and_read on a non-rewind-safe device does -- so the
        # live value is plan state and must not outlive the plan.
        self._rewindable_flag: bool = initially_rewindable

        # Materialise this executor's state machine while this is still the
        # only thread with a reference, so that no later read from another
        # thread can race the WeakKeyDictionary insertion. Every write after
        # this happens on the event loop, which is why it needs no lock.
        _ = self._state

        # Subscriptions that last only as long as this plan. Making the
        # lifetime structural means teardown is nothing more than dropping this
        # executor, and it gives plan tokens a namespace of their own, so a
        # plan cannot unsubscribe a session callback by guessing an integer.
        # Ordering documents against the session's subscribers is the
        # dispatcher's own business, through its parent.
        self._dispatcher = dispatcher
        for name, funcs in normalize_subs_input(subs).items():
            for func in funcs:
                self._dispatcher.subscribe(func, name)

        # Reached for through RunEngine._run_bundlers by bluesky's own tests,
        # to inspect the runs a plan has open. Private because the bundler for
        # an open run is mid-composition: reading it says nothing stable, and
        # the documents it emits are the supported way to see a run.
        self._run_bundlers: dict[typing.Any, RunBundler] = {}  # open run -> bundler
        self._metadata_per_call: dict[typing.Any, typing.Any] = {}  # md for every run
        self.run_start_uids: list[typing.Any] = []  # RunStart uids generated
        self._run_tracing_spans: list[Span] = []  # open tracing spans

        # The suspenders this plan installs for itself, which this executor
        # owns outright and which write to the suspension just below. The durable
        # ones are the session's: they hold this plan up through the suspension
        # chain, so there is nothing to keep a copy of here -- and a copy would
        # go stale the moment one was installed while this plan was running.
        self._plan_suspenders: set[SuspenderBase] = set()

        # This plan's own suspension, the counterpart of its own dispatcher: a
        # suspender a plan installs holds up that plan alone. The session's is
        # shared with every plan it is running.
        self._suspension = suspension

        self._staged: set[typing.Any] = set()  # staged, not yet unstaged
        self._objs_seen: set[typing.Any] = set()  # every object seen in a Msg
        self._movable_objs_touched: set[typing.Any] = set()  # everything we 'set'
        self._groups: defaultdict[str, set[Callable[[], asyncio.Future]]] = defaultdict(set)
        self._status_objs: defaultdict[typing.Any, set[typing.Any]] = defaultdict(set)
        # Reached for through RunEngine._seen_wait_and_move_on_keys by
        # test_flyer.py, to assert nothing was left behind. Private because it
        # is bookkeeping for one command's warning, not a fact about the plan.
        self._seen_wait_and_move_on_keys: set[typing.Any] = set()

        # An exception instance or class, to be raised into the plan.
        self._exception: typing.Any = None
        self.interrupted: bool = False  # paused, aborted or failed
        # How the plan ended, and why. Public because they are the outcome, and
        # whoever ran the plan has to be able to read it: a `RunEngine` puts
        # them in the `RunEngineResult` it returns, and a headless caller that
        # has neither has nothing else to ask.
        self.exit_status: str = "success"  # optimistic default
        self.exit_reason: str = ""
        # What was thrown in to end the plan early, for whoever asks afterwards.
        # Separate from `_exception`, which is the transport: the run loop takes
        # that one and clears it on the way to throwing it into the plan, and it
        # is routinely gone before the caller that ended the plan can look.
        self.exit_exception: typing.Any = None
        self._reason: str = ""  # the reason `stop` was given, if it was
        self._deferred_pause_requested: bool = False  # pause at next 'checkpoint'

        self._command_registry = self._build_command_registry(commands, without_commands)

        # Load the plan last, so that a preprocessor seeing a half-built
        # executor is not a thing that can happen.
        self._plan = plan  # this ref is just used for metadata introspection
        if metadata:
            self._metadata_per_call.update(metadata)
        gen = ensure_generator(plan)
        for wrapper_func in preprocessors:
            gen = wrapper_func(gen)
        self._push_plan(gen)

    # The hooks are the session's; firing one, and checking whether it is set
    # at all, belongs to whoever has something to report -- which for all three
    # of these is the executor.

    def _on_state_change(self, value, old_value) -> None:
        """Say that this plan changed state, in the name of whoever drives it."""
        announce_state_change(self._identity, self._hooks, old_value, value)

    def emit(self, name, doc) -> None:
        """Give a document to every subscriber that should see it.

        May be called from a thread that is not the event loop's: a sync ophyd
        signal fires its monitor callback on the device's own thread, and that
        path reaches here. Subscribers are therefore invoked on whichever
        thread emitted, which is not always the loop.
        """
        self._dispatcher.process(name, doc)

    @property
    def _loop(self) -> asyncio.AbstractEventLoop:
        """The event loop this plan is executed on."""
        return self._env.loop

    @property
    def suspenders(self) -> tuple[SuspenderBase, ...]:
        """The suspenders this plan installed for itself, which end with it.

        Not the session's. Those hold this plan up through the suspension chain,
        and whoever wants both asks both -- which is what `RunEngine` does.
        """
        return tuple(self._plan_suspenders)

    def _drop_plan_suspender(self, suspender: SuspenderBase) -> None:
        """Uninstall a suspender this plan installed for itself."""
        self._plan_suspenders.discard(suspender)
        # `remove` drops the suspender's reason itself, on this suspension and under
        # this key. Granting again here would be the same write a second time --
        # and a bare one, made on whatever thread called in, which is how
        # `RunEngine.clear_suspenders` came to raise when reached from the
        # prompt.
        suspender.remove()

    def clear_suspenders(self) -> None:
        """Uninstall every suspender this plan installed for itself."""
        for suspender in list(self._plan_suspenders):
            self._drop_plan_suspender(suspender)

    def _arrange_permission(self) -> None:
        """Hold the plan if it may not run yet, and watch the suspension from here.

        Called at the start of a plan and again when one resumes, because both
        are entries into a running plan from a state where the user had control
        and the suspension may have moved without anything watching it.

        One read decides both halves, so they cannot disagree.
        """
        tripped = self._suspension.tripped
        # Held in band, ahead of the plan's first message. A suspension proper
        # cannot do this job: there is no checkpoint yet to rewind to, so
        # requesting one would abort the plan rather than hold it. A plan
        # already parked in that wait when it paused is the exception -- the run
        # loop re-sends the message on the way out, and a second would hold for
        # the same thing twice.
        if tripped and self._cleared_when_paused:
            self._push_plan(single_gen(Msg("wait_for", None, [self._suspension.wait_cleared])))
        if self._supervisor is not None:
            self._supervisor.cancel()
        self._supervisor = self._loop.create_task(self._supervise_suspension(tripped_at_start=tripped))

    @property
    def _run_task(self) -> asyncio.Task:
        """The task running the plan. Only ask while one is running."""
        if self._task is None:
            raise RuntimeError("No plan is running, so there is no task to interrupt.")
        return self._task

    async def _supervise_suspension(self, tripped_at_start=False):
        """Suspend the plan whenever its suspension is tripped.

        One suspension per episode, however many conditions are standing: a
        condition tripping while one is open joins it rather than starting a
        second rewind.
        """
        if tripped_at_start:
            # Already held in band by `_arrange_permission`, and there is no
            # checkpoint yet to rewind to, so this must not suspend for it.
            await self._suspension.wait_cleared()
        while True:
            # The read that decides is the read that reports, so this cannot be
            # told to suspend and then find nothing to suspend for. Nothing is
            # arranged while the plan is at rest or coming to rest either:
            # control has gone back to the user, and `resume` replaces this task.
            reasons = self._suspension.reasons
            if not reasons or self.state in ("paused", "pausing"):
                await self._suspension.wait_changed()
                continue
            opening = dict(reasons)
            self._hooks.suspend(opening)
            if not self._begin_suspension(opening):
                return
            # The plan runs the episode from here -- taking in whatever joins it
            # and running the pre-plans -- so this only waits for it to end
            # before another can be opened.
            await self._suspension.wait_cleared()

    def _begin_suspension(self, opening: dict[typing.Hashable, SuspensionReason]) -> bool:
        """Put a suspension for ``opening`` in front of the plan.

        False if the plan cannot be held.
        """
        if self.state.is_idle:
            # The plan ended before this was reached. Nothing to suspend, and
            # neither transition below is legal from 'idle'.
            return False
        if not self.resumable:
            # Nothing to rewind to. The plan stack is being torn down, so a
            # suspension queued onto it would never be reached.
            self._hooks.announce("No checkpoint; cannot suspend.")
            self._hooks.announce("Aborting: running cleanup and marking exit_status as 'abort'...")
            self.interrupted = True
            self._exception = FailedPause()
            was_paused = self.state == "paused"
            self.state = "aborting"
            if not was_paused:
                self._run_task.cancel()
            return False

        self._push_plan(single_gen(Msg("_start_suspender", None, opening)))
        # Not from 'paused': the transition is illegal, and there is nothing
        # awaiting to bump -- the plan is parked in the pause gate, and `resume`
        # replays it onto the message just pushed. Only the suite's
        # `force_suspension` reaches here that way, because the supervisor
        # arranges nothing while the plan is at rest.
        if self.state != "paused":
            self.state = "suspending"
            # Bump the run task out of whatever it is awaiting, so that it
            # reaches the message just pushed.
            self._run_task.cancel()
        return True

    @property
    def suspensions(self) -> typing.Mapping[typing.Hashable, SuspensionReason]:
        """What is holding this plan up, by who raised it.

        The whole chain: conditions raised on the session as well as ones this
        plan installed for itself. Empty when nothing is holding it.
        """
        return self._suspension.reasons

    @property
    def resumable(self) -> bool:
        "i.e., can the plan in progress be rewound"
        return self._msg_cache is not None

    @property
    def rewindable(self) -> bool:
        """Whether messages may be replayed on a rewind.

        Owned by this executor and seeded from the session's default, because
        plans change it for their own duration -- see
        `bluesky.preprocessors.rewindable_wrapper`, which every
        ``trigger_and_read`` on a non-rewind-safe device goes through.
        """
        return self._rewindable_flag

    @rewindable.setter
    def rewindable(self, value: bool) -> None:
        # Changing this invalidates the message cache, because the point of
        # turning it off is that what follows must not be replayed. Both
        # writers -- Msg('rewindable') and RunEngine.rewindable -- come through
        # here, so there is one definition of what the change means.
        cur_state = self._rewindable_flag
        self._rewindable_flag = bool(value)
        if self.resumable and self._rewindable_flag != cur_state:
            self._reset_checkpoint_state()

    @property
    def deferred_pause_requested(self) -> bool:
        """Whether a deferred pause is waiting for the next checkpoint."""
        return self._deferred_pause_requested

    def _push_plan(self, plan) -> None:
        """Put a plan on the stack, with nothing answered for it yet.

        The two stacks are the same length by construction -- `run` asserts it
        every turn -- because a response is what the plan's last message got
        back, and a plan just pushed has not sent one.
        """
        self._plan_stack.append(plan)
        self._response_stack.append(None)

    @property
    def state(self):
        """This plan's state. One of {'idle', 'running', 'paused', ...}.

        Belongs to the executor rather than the session because every non-idle
        value describes one plan's execution: `pausing`, `suspending`,
        `aborting`, `stopping` and `halting` all mean that somebody outside the
        run loop has asked *this* plan to stop.
        """
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
                    # The device will not be replayed through, so there is
                    # nothing to rewind to any more.
                    self._reset_checkpoint_state()

    async def _resume_objects(self) -> None:
        """The plan is moving again: tell the devices, so they can prepare.

        Only the ways back *in* call this. Abort, stop and halt let a paused
        plan go so that it can unwind, which is not resuming, and the devices
        are told nothing.
        """
        for obj in self._objs_seen:
            if isinstance(obj, Pausable):
                await maybe_await(obj.resume())

    async def _stop_movable_objects(self, *, success=True):
        "Call obj.stop() for all objects we have moved. Log any exceptions."
        for obj in self._movable_objs_touched:
            if isinstance(obj, Stoppable):
                try:
                    await maybe_await(obj.stop(success=success))
                except Exception:
                    self._env.log.exception("Failed to stop %r.", obj)
            else:
                self._env.log.debug("No 'stop' method available on %r", obj)

    def _destroy_open_run_tracing_spans(self):
        while len(self._run_tracing_spans):
            _span = self._run_tracing_spans.pop()
            _span.set_attribute("exit_status", "aborted")
            _span.end()

    async def run(self):
        """Execute the plan this executor was built for.

        Awaiting this is all that is needed to run a plan::

            executor = session.make_executor(plan)
            result = await executor.run()

        Returns
        -------
        The value the plan returned, or :data:`NO_PLAN_RETURN` if it did not
        run to completion.

        Raises
        ------
        RuntimeError
            If called twice. An executor is for one plan; caches like
            ``run_start_uids`` and ``_pardon_failures`` are never reset, so a
            second run would accumulate uids and silently pardon the second
            plan's status failures.

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
        if self._spent:
            raise RuntimeError(
                f"{self!r} has already run its plan. Build another executor: one executor runs one plan."
            )
        self._spent = True
        # grab the current task.  We need to do this here because the
        # object returned by `run_coroutine_threadsafe` is a future
        # that acts as a proxy that does not have the correct behavior
        # when `.cancel` is called on it.
        self._task = asyncio.current_task(self._env.loop)
        self._arrange_permission()
        stashed_exception = None
        debug = msg_logger.debug
        self._reason = ""
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
                    # Whether the plan was already waiting on its suspension when
                    # it came to rest. If it was, the run loop re-sends that
                    # message on the way out and the plan holds itself; if it
                    # was not, anything tripped by then arrived during the
                    # pause, and `resume` has to arrange the wait.
                    self._cleared_when_paused = not self._suspension.tripped
                    self.state = "paused"
                    # Let RunEngine.__call__ return...
                    self._hooks.pause()

                    await self._run_permit.wait()
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
            if not self.exit_reason:
                self.exit_reason = self._reason
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

    def _close_run_trace(self, msg: Msg):
        exit_status = msg.kwargs.get("exit_status", self.exit_status)
        reason = msg.kwargs.get("reason", self._reason)
        try:
            _span: Span = self._run_tracing_spans.pop()
            _span.set_attribute("exit_status", exit_status if exit_status is not None else "None")
            _span.set_attribute("reason", reason if reason is not None else "None")
            _span.end()
        except IndexError:
            logger.warning("No open traces left to close!")

    def _status_object_completed(self, ret, fut: asyncio.Future, pardon_failures, obj=None, action=None):
        """
        Task to run when a status object is finished.

        Always called on the event loop, via the trampoline that
        :meth:`_add_status_to_group` hands to the status object.

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
                # We have set the exception, but we don't mind if
                # no-one collects it from the future, so fetch it ourselves to
                # squash "Future exception was never retrieved" at teardown.
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

        # A sync ophyd Status runs its callbacks on whichever thread completed
        # it, so this may be called from a thread that is not the loop's. It
        # does nothing but hop back onto the loop, which keeps
        # _status_object_completed, and so all of the state it touches, on the
        # loop thread. Any arguments the device passes are dropped: settle has
        # closed over what it needs. An ophyd-async status already calls back
        # on the loop, where call_soon_threadsafe remains correct.
        def done_callback(*args: typing.Any, **kwargs: typing.Any) -> None:
            loop.call_soon_threadsafe(settle)

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

    async def pause(self, defer=False):
        """Bring the plan to rest at a resting point. Must be called on the loop.

        The gate this closes is not touched here: the run loop closes it itself
        when the cancellation below reaches it, having seen the 'pausing'
        state. All this does is say which kind of interruption it is, and bump
        the loop out of whatever it is awaiting so it notices.
        """
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

    async def resume(self):
        """Continue a paused plan from its last checkpoint. On the loop.

        Rewinds, tells devices, then releases the plan.

        A condition that went bad while the plan was paused is waited for here
        rather than suspended around: returning from a pause is returning from
        the user having control, so the plan waits for permission the way one
        starting from idle does, and runs no pre-plans on the way back in.

        A `RunEngine` must still call this from inside its context managers, so
        that SIGINT handling is reinstalled before the plan moves again.
        """
        self.interrupted = False
        for current_run in self._run_bundlers.values():
            current_run.record_interruption("resume")
        self._push_plan(self._rewind())
        await self._resume_objects()
        # Ahead of the replayed messages, so the plan does not move until every
        # condition has cleared. No pre-plans on the way back in: there is
        # nothing here for one to reverse.
        self._arrange_permission()
        # Last, so the parked plan finds all of the above already arranged when
        # it wakes. The gate is never opened from anywhere but here and the
        # three ways of ending a plan: it is closed only by a plan pausing
        # itself, so the ways out are the ways back in.
        self._run_permit.set()

    async def stop(self, *, success: bool = True, finalize: bool = True, reason: str = "") -> None:
        """End the running plan. The three public verbs are the three modes.

        ``success`` says whether what the plan was doing counts as having
        worked, the way it does for `bluesky.protocols.Stoppable`: it decides
        whether the runs close as ``'success'`` or ``'abort'``.

        ``finalize`` says whether the plan may run its own cleanup -- the
        ``finally`` of a `bluesky.preprocessors.finalize_wrapper`, say. It does
        not gate this executor's teardown, which always runs: stopping movables,
        clearing monitors, unstaging, closing runs. The plan is stopped from
        cleaning up by *what is thrown into it*: `PlanHalt` is a `GeneratorExit`,
        so the plan cannot yield again once it arrives.

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
            Whether the plan may run its own cleanup on the way out.
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
        self._reason = reason
        if not success:
            # Set here and not left to the `except` clauses in `run`, which see
            # only what reaches them: a plan that catches what is thrown into it,
            # runs its cleanup and returns normally ends in `StopIteration`,
            # where those clauses would call the run a success. Saying it now
            # means the verb decides, not the plan's manners.
            self.exit_status = "abort"
            # Likewise only when the plan is being given up on. A stop is an
            # orderly end and closes its spans the ordinary way.
            self._destroy_open_run_tracing_spans()

        was_paused = self.state == "paused"
        self.state = state
        if was_paused:
            # A paused plan is parked at the gate, so raising the exception into
            # it is not enough on its own: it has to be let go before it can run
            # whatever cleanup it is being allowed.
            #
            # Recorded before it is handed over, because handing it over is what
            # loses it: releasing the pause lets the run loop take `_exception`
            # and clear it, which it usually does before the caller that asked
            # for the stop gets as far as asking how the plan ended.
            # An instance for abort, the class for stop and halt. That is what
            # `main` put in `RunEngineResult.exception`, and a caller reading it
            # can tell the difference -- `isinstance` answers for the one and
            # not the others -- so it is preserved rather than tidied. Only the
            # record: `_exception` stays the class it already was, because the
            # run loop compares it by identity with what comes back out of the
            # plan.
            self.exit_exception = RequestAbort() if exception is RequestAbort else exception
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
        # These tasks are ours: nothing else holds a reference to cancel them.
        # `asyncio.wait` does not cancel what it was waiting on when it is
        # itself cancelled, so a plan aborted while parked here left them
        # running on a loop about to be closed, and asyncio reported "Task was
        # destroyed but it is pending!" at some unrelated later moment.
        #
        # Only on the way out. A timeout leaves them alone deliberately: the
        # awaitables are built by factories that work more than once, and
        # waiting on the same group again after a timeout has to find whatever
        # it was waiting for still in flight.
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
        """The bundler for ``run_key``'s open run, or `IllegalMessageSequence`.

        The sentinel dance this replaces could not narrow: every caller went on
        to use a `RunBundler` that the type said might still be the sentinel.
        A bundler is never `None`, so absence is what `get` already reports.
        """
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

        # A run is opening, so ask for its scan id. Used as given, rather than
        # read back out of md: another executor may be opening a run on this
        # same session, and md holds whichever id was handed out last.
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
            self.emit,
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

                    def cancel_status_task_if_error(fut: asyncio.Future[list[asyncio.Future]]):
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
        """The suspension is over: tell the devices. Msg('_resume_from_suspender')

        Sent by the helper plan `_start_suspender` pushes, between the hold and
        the post-plan. Nothing to do with `RunEngine.resume`.

        Monitors are untouched: a suspension never stopped them.
        """
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
        """Install an ephemeral suspender. Msg('install_suspender', None, suspender)

        Ephemeral because it holds up this plan alone and is removed when the
        plan ends. `RunEngine.install_suspender` installs a persistent one,
        which holds up every plan the engine runs until it is removed.
        """
        suspender = msg.args[0]
        self._plan_suspenders.add(suspender)
        suspender.install(self._suspension)

    async def _remove_suspender(self, msg: Msg) -> typing.Any:
        """Remove a suspender from this plan. Msg('remove_suspender', None, suspender)

        Only a suspender this plan installed. One installed on the session
        outlives every plan and is not this one's to unsubscribe, and its reason
        stands on the session's suspension rather than this plan's.
        """
        suspender = msg.args[0]
        if not suspender.installed_on(self._suspension):
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
        # Snapshotted, so that a reason joining cannot quietly enlarge the set
        # the plan opened with, nor the order its pre-plans ran in.
        opening_order = list(opening.values())
        seen = set(opening)
        # Filled by the plan below as reasons join it, and read back for the
        # post-plans. It belongs to this episode and no message carries it: an
        # accumulator in `msg.args` would be shared with any replay of that
        # message.
        joined: list[SuspensionReason] = []

        async def a_change():
            """Park until the suspension clears, or a reason outside ``seen`` joins it."""
            while True:
                reasons = self._suspension.reasons
                if not reasons or reasons.keys() - seen:
                    return
                # Nothing may await between that test and this wait: a condition
                # tripping in the gap would leave the plan parked here with a
                # joiner nobody has run.
                await self._suspension.wait_changed()

        def suspension():
            # None of this is replayed: rewinding is what happens after it.
            yield Msg("rewindable", None, False)
            # The openers' pre-plans, after the rewind and after movable
            # objects have stopped.
            for reason in opening_order:
                if reason.pre_plan is not None:
                    yield from ensure_generator(_called(reason.pre_plan))
            # Then hold, until the episode is released. A condition tripping
            # while the plan is held joins this episode rather than opening a
            # second one, and its pre-plan runs here, in band, on the way
            # through -- so it is the plan that runs it, on the plan stack,
            # with a checkpoint behind it and the run loop's error handling
            # around it. A paused plan reaches none of this, which is what
            # makes "nothing runs unprompted while paused" true rather than
            # merely intended.
            while True:
                yield Msg("wait_for", None, [a_change])
                joining = {key: reason for key, reason in self._suspension.reasons.items() if key not in seen}
                if not joining:
                    break
                for key, reason in joining.items():
                    seen.add(key)
                    joined.append(reason)
                    if reason.pre_plan is not None:
                        yield from ensure_generator(_called(reason.pre_plan))
            yield Msg("_resume_from_suspender", None)
            # Read now rather than when this generator was built, so that
            # everything which joined in the loop above is undone too.
            for reason in [*reversed(joined), *reversed(opening_order)]:
                if reason.post_plan is not None:
                    yield from ensure_generator(_called(reason.post_plan))
            yield Msg("rewindable", None, was_rewindable)
            yield from rewind_plan

        self._push_plan(suspension())

    # The built-in vocabulary, as command name -> the method that handles it.
    # The methods themselves, so that following one is a click rather than a
    # search, and unbound so that the table can be read without an executor to
    # bind to: `PlanSession.commands` reports what the next plan will
    # understand, and it holds no executor to ask. Defined below the handlers
    # because a class body cannot name a method it has not reached yet.
    _DEFAULT_COMMANDS: typing.ClassVar[dict[str, Callable[["PlanExecutor", Msg], Awaitable[typing.Any]]]] = {
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
        """The vocabulary this plan understands, composed once.

        The built-ins bound to this executor, then whatever was registered on
        top, less whatever was unregistered. Composed here and never again: a
        plan's meaning must not change under it, and nothing can change it from
        inside, since no `Msg` reaches the registry. Registering a command
        therefore takes effect for the next plan.
        """
        registry: dict[str, Callable[[Msg], Awaitable[typing.Any]]] = {
            name: fn.__get__(self) for name, fn in self._DEFAULT_COMMANDS.items()
        }
        registry.update(commands or {})
        for name in without_commands:
            registry.pop(name, None)
        return registry


def _set_span_msg_attributes(span, msg):
    span.set_attribute("msg.command", msg.command)
    span.set_attribute("msg.args", sanitize_np(msg.args))
    span.set_attribute("msg.kwargs", json.dumps(msg.kwargs, default=repr))
    span.set_attribute("msg.obj", repr(msg.obj)) if msg.obj else span.set_attribute("msg.no_obj_given", True)
