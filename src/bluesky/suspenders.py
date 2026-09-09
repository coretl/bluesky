import operator
from abc import ABCMeta, abstractmethod, abstractproperty
from warnings import warn

from bluesky.protocols import Subscribable

from .permits import Permit, running_on

# How long `RunEngine.install_suspender`, `remove_suspender` and
# `clear_suspenders` wait for the event loop to run the work they hand it,
# before giving up. Installing subscribes and removing unsubscribes, so a loop
# that never gets to them leaves the caller with no idea whether the signal is
# being watched.
SUBSCRIPTION_TIMEOUT = 10


class SuspenderBase(metaclass=ABCMeta):
    """An ABC to manage the callbacks between asyincio and pyepics.


    Parameters
    ----------
    signal : `ophyd.Signal` or `bluesky.protocols.Subscribable`
        The signal to watch for changes to determine if the
        scan should be suspended

    sleep : float, optional
        How long to wait in seconds after the resume condition is met
        before marking the event as done.  Defaults to 0

    pre_plan : iterable or iterator or generator function, optional
            a generator, list, or similar containing `Msg` objects, run when
            this suspender's condition goes bad. Make it idempotent: another
            condition may already have suspended the plan, and each suspender
            runs its own pre-plan when it fires.

    post_plan : iterable or iterator or generator function, optional
            a generator, list, or similar containing `Msg` objects, run before
            the plan resumes. Post-plans run in the reverse of the order their
            pre-plans did, so the last condition to go bad is the first undone.

    tripped_message : str, optional
        Message to include in the trip notification
    """

    def __init__(self, signal, *, sleep=0, pre_plan=None, post_plan=None, tripped_message=""):
        """ """
        # The permit to withhold while this reads as bad. A suspender does not
        # know what is running, or whether anything is: everything that must be
        # held up by this condition is waiting on that permit already.
        # Every piece of mutable state below -- the permit, whether this is
        # tripped, the last value it saw -- is written and read on the permit's
        # event loop and nowhere else. `install` and `remove` are called there,
        # and a reading arriving on a signal's own thread is handed over rather
        # than acted on. One sequence, on one thread: no lock, and no write
        # that a later one has to be able to supersede.
        self._permit = None
        self._tripped = False
        self._tripped_message = tripped_message
        self._sleep = sleep
        self._sig = signal
        self._pre_plan = pre_plan
        self._post_plan = post_plan
        self._last_value = None
        self._implements_protocol = isinstance(signal, Subscribable)

    def __repr__(self):
        return "{}({!r}, sleep={}, pre_plan={}, post_plan={}, tripped_message={})".format(  # noqa: UP032
            type(self).__name__,
            self._sig,
            self._sleep,
            self._pre_plan,
            self._post_plan,
            self._tripped_message,
        )

    def install(self, permit, *, event_type=None):
        """Subscribe to the signal, and withhold ``permit`` while it reads as bad.

        Parameters
        ----------

        permit : `bluesky.permits.Permit`
            Withheld while this suspender is tripped, and what decides how far
            the suspension reaches: a session's holds up every plan it runs, a
            plan's holds up that plan alone. Nothing hands one out, so this is
            reached through `RunEngine.install_suspender`,
            `bluesky.plan_executor.PlanSession.install_suspender`, or
            ``Msg('install_suspender')`` rather than called directly.

        event_type : str, optional
            The event type (subscription type) to watch. Only meaningful for a
            signal following ophyd's subscription pattern; a `Subscribable` one
            has no such notion, so passing it there is an error rather than
            something to ignore.

        Notes
        -----
        Call this on the permit's event loop. `RunEngine.install_suspender`
        crosses onto it for you, and is what a user should reach for; nothing
        here crosses on its own.
        """
        if self._implements_protocol and event_type is not None:
            # Checked before anything is recorded, and ahead of the deprecated
            # route below, which drops `event_type` on its way to
            # `install_suspender` and would otherwise ignore it silently.
            raise RuntimeError(f"Can not specify non-None event_type {event_type=} with Subscribable protocol")
        if not isinstance(permit, Permit):
            # Was `install(RE)` before suspension went through permits. Do what
            # it used to do, which is a durable install on that engine.
            warn(
                f"Passing a RunEngine to {type(self).__name__}.install is deprecated; "
                "it now takes the permit to withhold, which is not something a "
                "RunEngine hands out. Use RE.install_suspender(suspender).",
                DeprecationWarning,
                stacklevel=2,
            )
            permit.install_suspender(self)
            return
        if self._permit is not None:
            raise RuntimeError(
                f"This {type(self).__name__} is already installed. A suspender holds one permit, "
                "so installing it again would orphan the first: that permit would stay withheld "
                "with nothing left able to grant it, and this suspender would stop watching for "
                "whatever it was installed on. Call remove() first."
            )
        if not running_on(permit.loop):
            raise RuntimeError(
                f"{type(self).__name__}.install must be called on the permit's event loop, and "
                "this is not it. A suspender's state is written where its signal's readings are "
                "applied, so that the two cannot interleave, and it does not cross on its own. "
                "Use RunEngine.install_suspender(suspender), which crosses for you."
            )
        self._permit = permit
        # Both subscription styles call back with the current reading before
        # they return, and this is already the loop, so an already-bad signal
        # has withheld the permit by the time this returns. Waiting for beam
        # that is already down is what suspenders are for, and a caller that
        # installs one and then starts a plan must not be raced by its own trip.
        if self._implements_protocol:
            self._sig.subscribe_reading(self)
        elif callable(getattr(self._sig, "subscribe", None)):
            self._sig.subscribe(self, event_type=event_type, run=True)
        else:
            self._permit = None
            raise RuntimeError(
                "%s does not implement Subscribable protocol or adhere to ophyd subscription pattern." % self._sig
            )

    def remove(self):
        """Stop watching the signal, and drop whatever this was withholding.

        Call this on the permit's event loop, as with `install`.
        `RunEngine.remove_suspender` crosses onto it for you.
        """
        permit = self._permit
        if permit is not None and not running_on(permit.loop):
            raise RuntimeError(
                f"{type(self).__name__}.remove must be called on the permit's event loop, and "
                "this is not it. Use RunEngine.remove_suspender(suspender), which crosses for "
                "you. An uninstalled suspender may be removed from anywhere: there is no permit "
                "to write and no subscription to drop."
            )
        if permit is not None or not self._implements_protocol:
            # Nothing was subscribed if we were never installed, and a
            # Subscribable signal has no subscription to drop in that case.
            self._sig.clear_sub(self)
        self._permit = None
        self._tripped = False
        if permit is not None:
            # An uninstalled suspender must not go on suspending, and nothing
            # else will drop its reason once it has stopped watching its
            # signal. Every reading raised before this was applied before it,
            # so this is the last word on the permit.
            permit.grant(self)

    @abstractmethod
    def _should_suspend(self, value):
        """
        Determine if the current value of the signal is such
        that we need to tell the scan to suspend

        Parameters
        ----------
        value : object
            The value to evaluate to determine if we should
            suspend

        Returns
        -------
        suspend : bool
            True means suspend
        """
        raise NotImplementedError()

    @abstractmethod
    def _should_resume(self, value):
        """
        Determine if the scan is ready to automatically
        restart.

        Parameters
        ----------
        value : object
            The value to evaluate to determine if we should
            resume

        Returns
        -------
        suspend : bool
            True means resume
        """
        raise NotImplementedError()

    def __call__(self, value, **kwargs):
        """Make the class callable so that we can
        pass it off to the ophyd callback stack.

        This expects the massive blob that comes from ophyd

        A signal calls back on whatever thread it likes -- a channel access
        monitor thread, for one. This is where that thread meets the loop,
        because this is where the callback is defined; the reading is carried
        across and decided there, not here.
        """
        permit = self._permit
        if permit is None:
            return
        if self._implements_protocol:
            # Subscribable calls back with {name: Reading}
            value = value[self._sig.name]["value"]
        loop = permit.loop
        if running_on(loop):
            self.__decide(value)
        else:
            loop.call_soon_threadsafe(self.__decide, value)

    def __decide(self, value):
        """Work out what ``value`` means for the permit. On the loop.

        The reading is the one the signal called back with, carried over rather
        than read again here, so a justification reports the value that
        actually tripped this rather than whatever the signal has since become.
        """
        permit = self._permit
        if permit is None:
            # Uninstalled between the callback and this running.
            return
        self._last_value = value
        if self._should_suspend(value):
            was_tripped = self._tripped
            self._tripped = True
            if not was_tripped:
                permit.withhold(
                    self,
                    self._get_justification(),
                    pre_plan=self._pre_plan,
                    post_plan=self._post_plan,
                )
        elif self._should_resume(value):
            if self._tripped:
                # Only release what actually tripped. Subscribing with
                # ``run=True`` calls back with the current reading, and an
                # already-nominal signal must not schedule a release: it would
                # come due `sleep` seconds later and drop a reason raised by a
                # trip in between.
                permit.grant(self, after=self._sleep)
            self._tripped = False

    @property
    def tripped(self):
        return self._tripped

    def _get_justification(self):
        if not self.tripped:
            return ""

        template = "Suspender of type {} stopped by signal {!r}"
        just = template.format(self.__class__.__name__, self._sig)
        return ": ".join(s for s in (just, self._tripped_message) if s)


class SuspendBoolHigh(SuspenderBase):
    """
    Suspend when a boolean signal goes high; resume when it goes low.

    Parameters
    ----------
    signal : `ophyd.Signal`
        The signal to watch for changes to determine if the
        scan should be suspended

    sleep : float, optional
        How long to wait in seconds after the resume condition is met
        before marking the event as done.  Defaults to 0

    pre_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects

    post_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects
    """

    def _should_suspend(self, value):
        return bool(value)

    def _should_resume(self, value):
        return not bool(value)

    def _get_justification(self):
        if not self.tripped:
            return ""

        just = f"Signal {self._sig.name} is high"
        return ": ".join(s for s in (just, self._tripped_message) if s)


class SuspendBoolLow(SuspenderBase):
    """
    Suspend when a boolean signal goes low; resume when it goes high.

    Parameters
    ----------
    signal : `ophyd.Signal`
        The signal to watch for changes to determine if the
        scan should be suspended

    sleep : float, optional
        How long to wait in seconds after the resume condition is met
        before marking the event as done.  Defaults to 0

    pre_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects

    post_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects
    """

    def _should_suspend(self, value):
        return not bool(value)

    def _should_resume(self, value):
        return bool(value)

    def _get_justification(self):
        if not self.tripped:
            return ""

        just = f"Signal {self._sig.name} is low"
        return ": ".join(s for s in (just, self._tripped_message) if s)


class _Threshold(SuspenderBase):
    """
    Private base class for suspenders that watch when a scalar
    signal fall above or below a threshold.  Allow for a possibly different
    threshold to resume.
    """

    def __init__(self, signal, suspend_thresh, *, resume_thresh=None, **kwargs):
        super().__init__(signal, **kwargs)
        self._suspend_thresh = suspend_thresh
        if resume_thresh is None:
            resume_thresh = suspend_thresh
        self._resume_thresh = resume_thresh
        self._validate()

    def _should_suspend(self, value):
        return self._op(value, self._suspend_thresh)

    def _should_resume(self, value):
        return not self._op(value, self._resume_thresh)

    @abstractproperty
    def _op(self):
        pass

    @abstractmethod
    def _validate(self):
        pass


class SuspendFloor(_Threshold):
    """
    Suspend when a scalar falls below a threshold.

    Optionally, the threshold to resume can be set to be greater than the
    threshold to suspend.

    Parameters
    ----------
    signal : `ophyd.Signal`
        The signal to watch for changes to determine if the
        scan should be suspended

    suspend_thresh : float
        Suspend if the signal value falls below this value

    resume_thresh : float, optional
        Resume when the signal value rises above this value.  If not
        given set to `suspend_thresh`.  Must be greater than `suspend_thresh`.

    sleep : float, optional
        How long to wait in seconds after the resume condition is met
        before marking the event as done.  Defaults to 0

    pre_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects

    post_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects
    """

    def _validate(self):
        if self._resume_thresh < self._suspend_thresh:
            raise ValueError(
                "Resume threshold must be equal or greater "
                "than suspend threshold, you passed: "
                f"suspend: {self._suspend_thresh}  resume: {self._resume_thresh}"
            )

    @property
    def _op(self):
        return operator.lt

    def _get_justification(self):
        if not self.tripped:
            return ""

        just = (
            f"Signal {self._sig.name} = {self._last_value!r} "
            + f"fell below {self._suspend_thresh} "
            + f"and has not yet crossed above {self._resume_thresh}."
        )
        return ": ".join(s for s in (just, self._tripped_message) if s)


class SuspendCeil(_Threshold):
    """
    Suspend when a scalar rises above a threshold.

    Optionally, the threshold to resume can be set to be less than the
    threshold to suspend.

    Parameters
    ----------
    signal : `ophyd.Signal`
        The signal to watch for changes to determine if the
        scan should be suspended

    suspend_thresh : float
        Suspend if the signal value falls below this value

    resume_thresh : float, optional
        Resume when the signal value rises above this value.  If not
        given set to `suspend_thresh`.  Must be greater than `suspend_thresh`.

    sleep : float, optional
        How long to wait in seconds after the resume condition is met
        before marking the event as done.  Defaults to 0

    pre_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects

    post_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects
    """

    def _validate(self):
        if self._resume_thresh > self._suspend_thresh:
            raise ValueError(
                "Resume threshold must be equal or less "
                "than suspend threshold, you passed: "
                f"suspend: {self._suspend_thresh}  resume: {self._resume_thresh}"
            )

    @property
    def _op(self):
        return operator.gt

    def _get_justification(self):
        if not self.tripped:
            return ""

        just = (
            f"Signal {self._sig.name} = {self._last_value!r} "
            + f"went above {self._suspend_thresh} "
            + f"and has not yet crossed below {self._resume_thresh}."
        )
        return ": ".join(s for s in (just, self._tripped_message) if s)


class _SuspendBandBase(SuspenderBase):
    """
    Private base-class for suspenders based on keeping a scalar inside
    or outside of a band
    """

    def __init__(self, signal, band_bottom, band_top, **kwargs):
        super().__init__(signal, **kwargs)
        if not band_bottom < band_top:
            raise ValueError(
                "The bottom of the band must be strictly "
                "less than the top of the band.\n"
                f"bottom: {band_bottom}\ttop: {band_top}"
            )
        self._bot = band_bottom
        self._top = band_top


class SuspendWhenOutsideBand(_SuspendBandBase):
    """
    Suspend when a scalar signal leaves a given band of values.

    Parameters
    ----------
    signal : `ophyd.Signal`
        The signal to watch for changes to determine if the
        scan should be suspended

    band_bottom, band_top : float
        The top and bottom of the band.  `band_top` must be
        strictly greater than `band_bottom`.

    sleep : float, optional
        How long to wait in seconds after the resume condition is met
        before marking the event as done.  Defaults to 0

    pre_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects

    post_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects
    """

    def _should_resume(self, value):
        return self._bot < value < self._top

    def _should_suspend(self, value):
        return not (self._bot < value < self._top)

    def _get_justification(self):
        if not self.tripped:
            return ""

        just = "Signal {} = {!r} is outside of the range ({}, {})".format(  # noqa: UP032
            self._sig.name, self._last_value, self._bot, self._top
        )
        return ": ".join(s for s in (just, self._tripped_message) if s)


class SuspendInBand(SuspendWhenOutsideBand):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        warn(  # noqa: B028
            "SuspendInBand has been renamed SuspendWhenOutsideBand to make "
            "its meaning more clear. Its behavior has not changed."
        )


class SuspendOutBand(_SuspendBandBase):
    """
    Suspend when a scalar signal enters a given band of values.

    This is mostly here because it is the opposite of `SuspenderInBand`.

    Parameters
    ----------

    signal : `ophyd.Signal`
        The signal to watch for changes to determine if the
        scan should be suspended

    band_bottom, band_top : float
        The top and bottom of the band.  `band_top` must be
        strictly greater than `band_bottom`.

    sleep : float, optional
        How long to wait in seconds after the resume condition is met
        before marking the event as done.  Defaults to 0

    pre_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects

    post_plan : iterable or iterator, optional
            a generator, list, or similar containing `Msg` objects
    """

    def __init__(self, *args, **kwargs):
        warn("bluesky.suspenders.SuspendOutBand is deprecated.")  # noqa: B028
        super().__init__(*args, **kwargs)

    def _should_resume(self, value):
        return not (self._bot < value < self._top)

    def _should_suspend(self, value):
        return self._bot < value < self._top

    def _get_justification(self):
        if not self.tripped:
            return ""

        just = "Signal {} = {!r} is inside of the range ({}, {})".format(  # noqa: UP032
            self._sig.name, self._last_value, self._bot, self._top
        )
        return ": ".join(s for s in (just, self._tripped_message) if s)


class SuspendWhenChanged(SuspenderBase):
    """
    Suspend when the monitored value deviates from the expected.

    Only resume if allowed AND when monitored equals expected.

    Notes
    -----

    This suspender is designed to require bluesky restart if value changes.

    USE CASE:

    :class:`~SuspendWhenChanged()` is useful when ``signal`` is an EPICS enumeration
    (`"mbbo" <https://wiki-ext.aps.anl.gov/epics/index.php/RRM_3-14_Multi-Bit_Binary_Output>`_)
    used with a multi-instrument facility.
    Choices predefined in the mbbo record are the
    names of instruments allowed to control any shared hardware.

    * The ``signal``, set by instrument staff outside of bluesky,
      names which instrument is allowed to control the hardware.
    * Other instruments not matching ``signal`` are expected **not** to
      control the hardware (they could use simulators instead or not operate
      the shared hardware).

    Since a decision of hardware *vs.* simulators is made at the
    time a bluesky session starts and ophyd objects are first created, the
    session needs to be aware immediately if the ``signal`` is changed.
    The default value of ``allow_resume=False`` defends this decision.
    If there is a mechanism engineered to toggle ophyd signals between
    hardware and simulators, one might consider ``allow_resume=True``.


    Parameters
    ----------

    signal : `ophyd.Signal` or `bluesky.protocols.Subscribable`
        The signal to watch for changes to determine if the
        scan should be suspended

    expected_value : str, float, or int
        RunEngine operations will be suspended when signal deviates
        from this value.  If `None` (default), set to the first value the
        signal reports, when the object is installed on a RunEngine.  Until
        then it stays `None`, whatever kind of signal this is watching.

    allow_resume : bool
        Should RunEngine be allowed to resume once ``signal.value == expected``
        again?  Default value of ``False`` is expected for intended use case.

    sleep : float, optional
        How long to wait in seconds after the resume condition is met
        before marking the event as done.  Defaults to ``0``.

    pre_plan : iterable or callable, optional
       Plan to execute just before suspending. If callable, must
       take no arguments.

    post_plan : iterable or callable, optional
        Plan to execute just before resuming. If callable, must
        take no arguments.

    tripped_message : str, optional
        Message to include in the trip notification


    Examples
    --------

    .. code-block:: python

        # pause if this value changes in our session
        # note: this suspender is designed to require Bluesky restart if value changes
        suspend_instrument_in_use = SuspendWhenChanged(instrument_in_use)
        RE.install_suspender(suspend_instrument_in_use)

    Example EPICS database for APS 2-BM-A and 2-BM-B:

    .. code-block:: text

        record(mbbo, "2bm:instrument_in_use") {
            # instrument team sets this
            # For additional field names, see
            # https://epics.anl.gov/EpicsDocumentation/AppDevManuals/RecordRef/Recordref-25.html#HEADING25-15
            field(DESC, "instrument using beam now")
            field(ZRST, "none")
            field(ONST, "2-BM-A")
            field(TWST, "2-BM-B")
            # THST
            # FRST
            # FVST
            # ...
        }

    NOTE: **Always** make the zero choice (``ZRST``) in the mbbo record to be 'none'.
    This allows the instrument staff to designate that *no* instrument is allowed
    to control the shared hardware.  Start the names of the allowed instruments
    with ``ONST``.

    It is convenient for the multi-instrument facility to make this definition
    in EPICS rather than in a specific bluesky session.  The EPICS value could be
    useful in other contexts of instrument control beyond the realm of bluesky.
    """

    def __init__(
        self,
        signal,
        *,
        expected_value=None,
        allow_resume=False,
        sleep=0,
        pre_plan=None,
        post_plan=None,
        tripped_message="",
        **kwargs,
    ):
        self.expected_value = expected_value
        self.allow_resume = allow_resume
        super().__init__(
            signal, sleep=sleep, pre_plan=pre_plan, post_plan=post_plan, tripped_message=tripped_message, **kwargs
        )

    def _should_suspend(self, value):
        if self.expected_value is None:
            # Latched on install, from the reading both subscription styles call
            # back with before `install` returns. Reading an ophyd signal in
            # __init__ instead would make *when* the default is captured depend
            # on which protocol the signal happens to implement.
            self.expected_value = value
        return value != self.expected_value

    def _should_resume(self, value):
        return self.allow_resume and value == self.expected_value

    def _get_justification(self):
        if not self.tripped:
            return ""

        just = f'Signal {self._sig.name}, got "{self._last_value}", expected "{self.expected_value}"'
        if not self.allow_resume:
            just += '.  "RE.abort()" and then restart session to use new configuration.'
        return ": ".join(s for s in (just, self._tripped_message) if s)
