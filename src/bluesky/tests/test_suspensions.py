"""Unit tests for `Suspension` itself; behaviour through a RunEngine is in `test_suspenders.py`."""

import asyncio
import threading

from bluesky import Msg
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


def test_a_child_suspension_is_tripped_whenever_its_parent_is():
    """A child is tripped whenever its parent is."""

    async def check():
        loop = asyncio.get_running_loop()
        parent = Suspension("session", loop=loop)
        child = Suspension("plan", loop=loop, parent=parent)

        parent.trip("beam", "beam is down")
        # Held up by its parent.
        assert child.tripped
        assert join_justifications(child.reasons) == "beam is down"

        child.trip("shutter", "shutter is closed")
        parent.clear("beam")
        # Still holding its own reason.
        assert child.tripped
        assert not parent.tripped

        child.clear("shutter")
        assert not child.tripped

    asyncio.run(check())


def test_a_trip_between_building_the_plan_and_running_it_still_holds():
    """A trip between `start` and the plan's first message still holds the plan."""
    from bluesky.plan_session import PlanSession

    steps = []

    def plan():
        yield Msg("checkpoint")
        for _ in range(3):
            steps.append("step")
            yield Msg("sleep", None, 0.05)

    async def main():
        session = PlanSession()
        runner = session.start(plan())
        # Nothing had tripped when this was built.
        assert not runner._suspension.tripped

        session._suspension.trip("beam", "beam is down")
        task = asyncio.ensure_future(runner)
        await asyncio.sleep(0.3)
        held = list(steps)

        session._suspension.clear("beam")
        await asyncio.wait_for(task, timeout=10)
        return held

    ran_while_tripped = asyncio.run(main())

    assert ran_while_tripped == []
    assert steps == ["step"] * 3
