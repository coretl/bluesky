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


def suspend_until(RE, fut, *, pre_plan=None, post_plan=None, justification=None):
    """Hold ``RE``'s running plan until ``fut`` completes.

    Deliberately not a method on `RunEngine`: ``RunEngine.request_suspend`` was
    one, and was removed. Installing a suspender is how a plan is suspended;
    the suite keeps this for a hold whose length the test decides, which a
    suspender cannot easily give.

    Callable from any thread, including a ``threading.Timer``.
    """
    RE._suspend(fut, pre_plan=pre_plan, post_plan=post_plan, justification=justification)


def force_suspension(RE, *, pre_plan=None, post_plan=None, justification=None):
    """Put a suspension in front of ``RE``'s plan with nothing holding it up.

    The hold is over before the plan reaches it, so what runs is the pre-plan,
    the rewind and the post-plan. The suite keeps it for the conditions a
    suspender cannot produce at all -- a malformed pre-plan, a pre-plan that
    raises, a plan with no checkpoint to rewind to.

    Callable from any thread, including a ``threading.Timer``.
    """
    RE._suspend(None, pre_plan=pre_plan, post_plan=post_plan, justification=justification)
