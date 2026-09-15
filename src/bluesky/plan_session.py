"""The environment plans are executed in, and what outlives any one of them."""

import asyncio
import typing
from logging import LoggerAdapter

import event_model

from .bundlers import RunBundler, maybe_await
from .dispatcher import Dispatcher
from .log import ComposableLogAdapter, logger
from .plan_executor import (
    PlanEnvironment,
    PlanExecutor,
    PlanHooks,
    RunEngineMetadata,
    _default_event_loop,
    _default_md_normalizer,
    _default_md_validator,
    default_scan_id_source,
)
from .protocols import SyncOrAsync
from .suspenders import SuspenderBase
from .suspensions import Suspension, SuspensionReason

__all__ = ["PlanSession"]


class PlanSession:
    """The environment that plans are executed in.

    A session holds everything that outlives any single plan: the persistent
    metadata, the document routing, the suspenders and the hooks. It does not
    execute anything itself; a :class:`PlanExecutor` does that, and one session
    builds many, one per plan::

        executor = session.make_executor(my_plan())
        result = await executor.run()

    A `RunEngine` composes a session with the machinery needed to drive it
    from a terminal on the main thread, and uses that same pair of calls. A
    session needs no `RunEngine`, so it can also be used directly from code
    that is already running in an asyncio event loop, such as a headless data
    acquisition service.

    A session does not hold the executors it builds, so a headless caller can
    run two plans at once against one set of durable metadata and suspenders.
    A `RunEngine` drives exactly one, and enforces that itself.

    Parameters
    ----------
    md : MutableMapping[str, Any], optional
        The default is a standard Python dictionary, but fancier objects can
        be used to store long-term history and persist it between sessions.
        Any object adhering to the MutableMapping Protocol will work.

    loop : asyncio event loop, optional
        The loop plans will be executed on. Defaults to the running loop, or
        to the loop a `RunEngine` has already established.

    log : logging.LoggerAdapter, optional
        Where this session and its executors log to.

    run_bundler_cls : type, optional
        The bundler used to compose documents for each open run. A
        `RunEngine` passes its own, so that overriding it on a `RunEngine`
        subclass keeps working.

    identity : object, optional
        What a state change is logged as having happened to, and what
        ``Msg('RE_class')`` reports the class of. A `RunEngine` passes itself,
        because that is what a user recognises in their logs; without one each
        executor answers for itself.

    An argument here is a setting nothing changes once the session exists.
    Everything else is an attribute you assign, read by `make_executor` as it
    freezes each plan's `PlanEnvironment`, so that a change takes effect for the
    next plan and never the one already running.

    Attributes
    ----------
    preprocessors
        Generator functions that take in a plan (generator instance) and
        modify its messages on the way out. Functions are composed in order:
        the preprocessors ``[f, g]`` are applied like ``f(g(plan))``.

    md_validator
        A function that raises and prevents starting a run if it deems the
        metadata to be invalid or incomplete.

    md_normalizer
        A function that, like ``md_validator``, raises to prevent starting a
        run, but which returns the normalized metadata if it succeeds.

    scan_id_source
        A (possibly async) function used to calculate ``scan_id``.

    ignore_exceptions
        Whether a raising subscriber is warned about rather than raised. One
        setting for the session and every plan running under it.

    hooks
        The `PlanHooks` record shared with every executor this session builds.
        One mutable record rather than a copy per plan, so setting a hook on it
        mid-plan takes effect on the plan already running.

    suspenders
        Read-only collection of the durable
        `bluesky.suspenders.SuspenderBase` objects, which every executor this
        session builds is given. Suspenders installed from inside a plan
        belong to that plan's executor and are not here.

    suspensions
        What is holding up every plan this session runs, and who raised each.
        Empty when nothing is. Not to be confused with ``suspenders``, which
        are the installed watchers; these are the holds standing right now.

    md
        Persistent metadata, surviving every plan, and the counter behind
        ``scan_id``. Not necessarily a ``dict``: `bluesky.utils.PersistentDict`
        is a supported choice, and is never replaced -- each plan is given a
        copy of its contents to read.

    The rest are the settings read as each plan's `PlanEnvironment` is built:
    ``preprocessors``, ``md_validator``,
    ``md_normalizer``, ``run_bundler_cls``, ``identity``,
    ``record_interruptions``, ``strict_pre_declare``, and ``rewindable`` --
    the last being only the *default*, since a running plan owns its own live
    value in `PlanExecutor.rewindable`.
    """

    def __init__(
        self,
        md: RunEngineMetadata | None = None,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        log: LoggerAdapter | None = None,
        run_bundler_cls: type[RunBundler] = RunBundler,
        identity: typing.Any = None,
    ):
        if loop is None:
            loop = _default_event_loop()
        self._loop = loop

        self.log = log if log is not None else ComposableLogAdapter(logger, {"RE": self})

        # Set up before the environment is built, because the environment is
        # where it then lives: one mapping, held in one place.
        if md is None:
            md = {}
        md.setdefault("versions", {})

        try:
            import ophyd

            md["versions"]["ophyd"] = ophyd.__version__
        except ImportError:
            self.log.debug("Failed to import ophyd.")

        try:
            import ophyd_async

            md["versions"]["ophyd_async"] = ophyd_async.__version__
        except ImportError:
            self.log.debug("Failed to import ophyd_async.")

        from ._version import __version__

        md["versions"]["bluesky"] = __version__
        md["versions"]["event_model"] = event_model.__version__

        self.scan_id_source: typing.Callable[[RunEngineMetadata], SyncOrAsync[int]] = default_scan_id_source
        # Serialises scan id allocation. Computing the next id reads md and
        # writes it back with an await in between, so two executors opening a
        # run at the same moment would otherwise be handed the same number.
        self._scan_id_lock = asyncio.Lock()

        # The observation points, shared by reference with every executor this
        # session builds, so that setting one mid-plan takes effect on that
        # plan. Set them on this record rather than through a constructor
        # argument each: `session.hooks.pause = f` reaches a running plan,
        # which is the whole point of holding them in one mutable place.
        self.hooks = PlanHooks()

        # Settings a plan is run under. Plain attributes, read when
        # `make_executor` builds the frozen `PlanEnvironment` it hands over,
        # so changing one here takes effect for the next plan and never for
        # the one already running. Held once, so nothing can drift out of
        # step with a copy of itself.
        self.md = md
        self.preprocessors: list = []
        self.md_validator: typing.Callable = _default_md_validator
        self.md_normalizer: typing.Callable = _default_md_normalizer
        self.run_bundler_cls = run_bundler_cls
        self.identity = identity
        self.record_interruptions = False
        self.strict_pre_declare = False
        self.rewindable = True

        self._suspenders: set[SuspenderBase] = set()
        # The durable half of the suspension state. Suspenders installed here
        # write to this suspension, and every executor this session builds waits
        # on it as well as on its own -- the same shape as the two dispatchers.
        self._suspension = Suspension("session", loop)

        # Commands the user has added or removed. Composed into each executor's
        # vocabulary as it is built, so that registrations survive the plan
        # that was running when they were made.
        self._registered_commands: dict[str, typing.Callable] = {}
        self._unregistered_commands: set[str] = set()

        # Documents go out through this; `subscribe` is the way in from outside.
        self._dispatcher = Dispatcher()

    async def _next_scan_id(self) -> int:
        """Compute the ``scan_id`` for a run that is opening, and return it.

        Returned rather than left in ``md`` for the caller to read back: two
        executors may be opening runs at once, and each must use the id it was
        given. It is stored in ``md`` as well, under a lock held across the
        ``await``, which is what makes the default source count up.
        """
        async with self._scan_id_lock:
            scan_id = await maybe_await(self.scan_id_source(self.md))
            self.md["scan_id"] = scan_id
            return scan_id

    @property
    def suspenders(self) -> tuple[SuspenderBase, ...]:
        """Read-only collection of installed suspenders."""
        return tuple(self._suspenders)

    @property
    def suspensions(self) -> typing.Mapping[typing.Hashable, SuspensionReason]:
        """What is holding up every plan this session runs, by who raised it.

        Empty when nothing is: the plans this session runs are suspended
        exactly when this is not. Ordered as the suspensions were raised, which
        is the order their pre-plans ran and the reverse of the order their
        post-plans will.
        """
        return self._suspension.reasons

    def register_command(self, name, func):
        """Register a new Message command.

        The session remembers it, so that it survives being composed over a
        different set of built-ins when the next executor is built.

        Parameters
        ----------
        name : str
        func : callable
            This can be a function or a method. The signature is ``f(msg)``.
        """
        self._registered_commands[name] = func
        self._unregistered_commands.discard(name)

    def unregister_command(self, name):
        """Unregister a Message command.

        Parameters
        ----------
        name : str
        """
        # Built-ins can be unregistered too, and are not in the registry, so
        # membership is asked of the names rather than of what has been added.
        if name not in self.commands:
            raise KeyError(name)
        self._registered_commands.pop(name, None)
        self._unregistered_commands.add(name)

    @property
    def commands(self) -> tuple[str, ...]:
        """The names of the commands the next plan will understand.

        `PlanExecutor`'s built-ins plus whatever has been registered here, less
        whatever has been unregistered. Names only: the callable a name resolves
        to is bound to the executor running the plan, and this session holds no
        executor to bind one to.
        """
        names = set(PlanExecutor._DEFAULT_COMMANDS) | set(self._registered_commands)
        return tuple(sorted(names - self._unregistered_commands))

    def _command_docs(self) -> dict[str, str | None]:
        """Docstring per command name, for `RunEngine.print_command_registry`."""
        registry: dict[str, typing.Callable] = dict(PlanExecutor._DEFAULT_COMMANDS)
        registry.update(self._registered_commands)
        return {name: registry[name].__doc__ for name in self.commands}

    def make_executor(self, plan, *, metadata=None, subs=None) -> "PlanExecutor":
        """Build an executor for ``plan``, and hand it to the caller.

        The caller owns what comes back; this session keeps no reference, so
        more than one executor can be running against one session at a time.
        A `RunEngine` keeps exactly one, and is where "one plan at a time" is
        enforced.

        Building a new executor is also how the previous plan's state is
        cleared, subscriptions included: those live on the executor's own
        dispatcher and are discarded with it.

        Parameters
        ----------
        plan : iterable of Msg
            The plan the new executor will run. Malformed plans raise here, on
            the calling thread.
        metadata : dict, optional
            Metadata for every run the plan opens.
        subs : callable, list, or dict, optional
            Subscriptions lasting only as long as this plan. Same forms as
            :meth:`RunEngine.__call__` accepts.

        """
        # This plan's own suspension, under the session's. A suspender the plan
        # installs holds up this plan; one installed on the session holds up
        # every plan it runs, and the chain is what makes those one mechanism.
        # An already-tripped suspension holds the plan at its first message; the
        # executor arranges that for itself.
        suspension = Suspension("plan", self._loop, parent=self._suspension)

        executor = self._build(plan, suspension, metadata=metadata, subs=subs)
        executor._begin()
        return executor

    def _idle_executor(self) -> "PlanExecutor":
        """An executor with no plan running, for a caller that needs one to read.

        A `RunEngine` keeps one between plans so that "no plan yet" is not a
        third state every caller has to reason about: it reports 'idle', which
        is what it means. It is never begun, because nothing would await it and
        a task parked for good would leave an object holding a plan that never
        ran.
        """
        return self._build((), Suspension("plan", self._loop, parent=self._suspension))

    def _build(self, plan, suspension, *, metadata=None, subs=None) -> "PlanExecutor":
        """Compose an executor for ``plan``, without setting it going."""
        return PlanExecutor(
            plan,
            # Built fresh for this plan, from the settings as they stand right
            # now. Frozen once handed over, so the plan cannot have its
            # environment changed under it, and never held by this session, so
            # there is no second copy of a setting to keep in step.
            PlanEnvironment(
                loop=self._loop,
                log=self.log,
                # A snapshot: a plan's environment does not change under it,
                # so writing to `session.md` takes effect for the next plan.
                # The contents only -- whatever store the session was given
                # stays the session's, and `next_scan_id` writes through to it.
                md=dict(self.md),
                next_scan_id=self._next_scan_id,
                md_validator=self.md_validator,
                md_normalizer=self.md_normalizer,
                run_bundler_cls=self.run_bundler_cls,
                record_interruptions=self.record_interruptions,
                strict_pre_declare=self.strict_pre_declare,
            ),
            suspension,
            self.hooks,
            # A dispatcher of this plan's own, under the session's, so that a
            # plan's subscribers end with it and its documents still reach the
            # session's. The chain is what makes those one mechanism.
            Dispatcher(parent=self._dispatcher),
            preprocessors=self.preprocessors,
            initially_rewindable=self.rewindable,
            metadata=metadata,
            subs=subs,
            identity=self.identity,
            commands=dict(self._registered_commands),
            without_commands=self._unregistered_commands,
        )

    def subscribe(self, func: typing.Callable, name: str = "all") -> int:
        """Register a callback to consume documents from every plan.

        Parameters
        ----------
        func : callable
            Expecting a signature like ``f(name, document)``, where name is a
            string and document is a dict.
        name : {'all', 'start', 'descriptor', 'event', 'stop'}, optional
            The type of document this function should receive ('all' by
            default).

        Returns
        -------
        token : int
            An integer ID that can be passed to :meth:`unsubscribe`.

        See Also
        --------
        :meth:`PlanSession.unsubscribe`
        """
        return self._dispatcher.subscribe(func, name)

    def unsubscribe(self, token: int) -> None:
        """Unregister a callback by the integer ID :meth:`subscribe` returned.

        See Also
        --------
        :meth:`PlanSession.subscribe`
        """
        self._dispatcher.unsubscribe(token)

    def unsubscribe_all(self) -> None:
        """Unregister every callback registered on this session.

        A plan's own subscribers are not reached: they belong to its executor
        and end with it.
        """
        self._dispatcher.unsubscribe_all()

    @property
    def ignore_exceptions(self) -> bool:
        """Whether a raising subscriber is warned about rather than raised.

        One setting for the session and every plan running under it: a plan's
        subscribers must not behave differently from the ones that outlive it,
        so setting this reaches the plans already running as well as the ones
        after them.
        """
        return self._dispatcher.ignore_exceptions

    @ignore_exceptions.setter
    def ignore_exceptions(self, val: bool) -> None:
        self._dispatcher.ignore_exceptions = val

    def install_suspender(self, suspender: SuspenderBase) -> None:
        """Install a durable suspender, given to every executor built after it.

        Installing subscribes the suspender to its signal here and now, and it
        stays subscribed between plans, so it can report a condition that was
        already bad when a plan started.

        It has no plan to suspend, and needs none: tripping holds up this
        session's suspension, which every executor it builds is waiting on.
        """
        self._suspenders.add(suspender)
        suspender.install(self._suspension)

    def remove_suspender(self, suspender: SuspenderBase) -> None:
        """Uninstall a durable suspender."""
        if suspender in self._suspenders:
            suspender.remove()
        self._suspenders.discard(suspender)

    def clear_suspenders(self) -> None:
        """Uninstall all suspenders."""
        for suspender in self.suspenders:
            self.remove_suspender(suspender)
