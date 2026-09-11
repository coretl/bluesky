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


def suspend_until(RE, fut=None, *, pre_plan=None, post_plan=None, justification=None):
    """Raise a suspension on ``RE``'s plan without going through a suspender.

    Not a supported route, and deliberately not a method on `RunEngine`: it
    reaches past the permit straight into the executor, so a suspension raised
    this way is not in ``RE.suspensions`` and does not merge with a suspender's.
    That is what ``RunEngine.request_suspend`` did, and why it was deleted.

    The suite keeps it for the conditions a suspender cannot easily produce: a
    malformed pre-plan, a pre-plan that raises, a plan with no checkpoint to
    rewind to. Anything testing ordinary suspension should install a suspender
    instead.

    Callable from any thread, including a ``threading.Timer``.
    """
    return asyncio.run_coroutine_threadsafe(
        RE._executor._request_suspend(fut, pre_plan=pre_plan, post_plan=post_plan, justification=justification),
        RE.loop,
    )
