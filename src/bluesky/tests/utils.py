import asyncio
import contextlib
import sys
import tempfile
import threading
from collections import defaultdict


@contextlib.contextmanager
def _print_redirect():
    old_stdout = sys.stdout
    try:
        fout = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
        sys.stdout = fout
        yield fout
    finally:
        sys.stdout = old_stdout


class MsgCollector:
    def __init__(self, msg_hook=None):
        self.msgs = []
        self.msg_hook = msg_hook

    def __call__(self, msg):
        self.msgs.append(msg)
        if self.msg_hook:
            self.msg_hook(msg)


class DocCollector:
    def __init__(self):
        self.start = []
        self.stop = {}
        self.descriptor = defaultdict(list)
        self.event = {}

    def insert(self, name, doc):
        if name == "start":
            self.start.append(doc)
        elif name == "stop":
            self.stop[doc["run_start"]] = doc
        elif name == "descriptor":
            self.descriptor[doc["run_start"]].append(doc)
            self.event[doc["uid"]] = []
        elif name == "bulk_events":
            for k, v in doc.items():
                self.event[k].extend(v)
        else:
            self.event[doc["descriptor"]].append(doc)


def _fabricate_asycio_event(loop):
    th_ev = threading.Event()

    aio_event = None

    def really_make_the_event():
        nonlocal aio_event
        aio_event = asyncio.Event()
        th_ev.set()

    h = loop.call_soon_threadsafe(really_make_the_event)
    if not th_ev.wait(0.1):
        h.cancel()
        raise Exception("failed to make asyncio event")
    return aio_event


def _careful_event_set(ev):
    "Helper to set 'do not lock test suite' backup sets"

    def inner():
        try:
            ev.set()
        except RuntimeError:
            ...

    return inner


def _at_message(RE, commands, **at):
    """Drive signals from the message stream rather than from the clock.

    ``msg_hook`` is called on the loop, for every message, so it is a place to
    make a condition go bad *at* a message instead of at a wall-clock instant
    that a loaded machine can miss. Each keyword names a command and gives a
    function to run the first time that command is seen -- first time only,
    because a suspension rewinds to the last checkpoint and replays, and the
    hook sees the replayed messages too.

    Setting a signal from here reaches the suspender on this loop: the set is a
    task, the trip is applied when it runs, and two sets made in one call are
    two tasks queued before the supervisor is woken by the first -- which is what
    makes "both conditions went bad in the same turn" a fact rather than a hope
    about two timers.
    """
    seen = set()

    def hook(msg):
        commands.append(msg.command)
        func = at.get(msg.command)
        if func is not None and msg.command not in seen:
            seen.add(msg.command)
            func()

    RE.msg_hook = hook


# `suspend_until`'s in-flight releases, kept alive. See the comment there.
_releases: set[asyncio.Task] = set()


def suspend_until(RE, fut, *, pre_plan=None, post_plan=None, justification=None):
    """Hold ``RE``'s running plan until ``fut`` completes.

    Trips the plan's suspension under a key of its own and clears it when
    ``fut`` does, so the plan's supervisor opens and ends the suspension exactly
    as it would one a suspender raised. Deliberately not a method on
    `RunEngine`: ``RunEngine.request_suspend`` was one, and was deleted.

    The suite keeps it for a hold whose length the test decides, which a
    suspender cannot easily give: the conditions a suspender cannot produce at
    all go through `force_suspension` instead.

    Callable from any thread, including a ``threading.Timer``.
    """
    key = object()

    async def begin():
        suspension = RE._suspension
        suspension.trip(key, justification or "", pre_plan=pre_plan, post_plan=post_plan)

        async def release():
            await fut()
            suspension.clear(key)

        # Held onto until it finishes: a task with no strong reference anywhere
        # can be collected mid-await, and this one is the only thing that ends
        # the hold.
        task = asyncio.ensure_future(release())
        _releases.add(task)
        task.add_done_callback(_releases.discard)

    return asyncio.run_coroutine_threadsafe(begin(), RE.loop)


def force_suspension(RE, *, pre_plan=None, post_plan=None, justification=None):
    """Put a suspension in front of ``RE``'s plan without tripping anything.

    Not a supported route: it reaches past the suspension straight into the
    runner, so the reason is not in ``RE.suspensions``, does not merge with a
    suspender's, and never reaches the supervisor -- which is the point, since
    the supervisor arranges nothing while a plan is paused and this must, to
    test what a pre-plan does on resume.

    Nothing here trips the suspension, so the hold it opens is already over by
    the time the plan reaches it: what runs is the pre-plan, the rewind and the
    post-plan. The suite keeps it for the conditions a suspender cannot produce
    -- a malformed pre-plan, a pre-plan that raises, a plan with no checkpoint
    to rewind to -- and `suspend_until` for a hold that lasts.

    Callable from any thread, including a ``threading.Timer``.
    """
    from bluesky.suspensions import SuspensionReason

    opening = {object(): SuspensionReason(justification or "", pre_plan, post_plan)}

    async def begin():
        RE._begin_suspension(opening)

    return asyncio.run_coroutine_threadsafe(begin(), RE.loop)
