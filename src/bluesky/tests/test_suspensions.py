"""Unit tests for `Suspension` itself; behaviour through a RunEngine is in `test_suspenders.py`."""

import asyncio
import threading

from bluesky.suspension import Suspension, join_justifications


def test_a_permit_is_read_from_any_thread():
    """Reads are safe from any thread: the reasons are swapped, not mutated."""
    loop = asyncio.new_event_loop()
    suspension = Suspension("test", loop=loop)

    async def trip():
        suspension.trip("beam", "beam is down")

    loop.run_until_complete(trip())

    seen = {}

    def read():
        seen["tripped"] = suspension.tripped
        seen["why"] = join_justifications(suspension.reasons)

    reader = threading.Thread(target=read)
    reader.start()
    reader.join()
    loop.close()

    assert seen == {"tripped": True, "why": "beam is down"}
