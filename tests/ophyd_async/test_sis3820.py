import asyncio
from unittest.mock import AsyncMock

import numpy as np
import pytest
from ophyd_async.core import (
    DetectorTrigger,
    TriggerInfo,
    callback_on_mock_put,
    get_mock_put,
    set_mock_value,
)
from ophyd_async.testing import wait_for_pending_wakeups

from hxntools.ophyd_async.sis3820 import Sis3820Mcs


def run(coro):
    return asyncio.run(coro)


async def make_sis():
    sis = Sis3820Mcs("SIM:SIS:", channel_numbers=(1, 2), name="sis")
    await sis.connect(mock=True)
    set_mock_value(sis.mcs.max_channels, 100)
    for mca in sis.mcs.mcas.values():
        set_mock_value(mca.nmax, 100)

    def arm(value):
        set_mock_value(sis.mcs.current_channel, 0)
        set_mock_value(sis.mcs.acquiring, True)

    callback_on_mock_put(sis.mcs.erase_start, arm)
    callback_on_mock_put(
        sis.mcs.stop_all, lambda value: set_mock_value(sis.mcs.acquiring, False)
    )
    await sis.stage()
    return sis


def info(count=4, **kwargs):
    return TriggerInfo(
        trigger=DetectorTrigger.EXTERNAL_LEVEL, number_of_events=count, **kwargs
    )


def publish(sis, count, channel1, channel2, *, nords=None):
    set_mock_value(sis.mcs.current_channel, count)
    for n, values in enumerate((channel1, channel2), start=1):
        set_mock_value(sis.mcs.mcas[n].data, np.asarray(values, dtype=np.int32))
        set_mock_value(
            sis.mcs.mcas[n].nord, len(values) if nords is None else nords[n - 1]
        )


async def pages(sis):
    return [page async for page in sis.collect_pages()]


def test_collection_requires_completed_acquisition():
    async def scenario():
        sis = await make_sis()
        await sis.prepare(info())
        await sis.kickoff()
        with pytest.raises(RuntimeError, match="Complete SIS3820 before collecting"):
            await pages(sis)
        await sis.unstage()

    run(scenario())


def test_completion_waits_for_barrier_and_preserves_final_counts():
    async def scenario():
        sis = await make_sis()
        await sis.prepare(info())
        await sis.kickoff()
        done = sis.complete()
        publish(sis, 4, [11, 22, 33, 16777217], [101, 102, 103, 104])
        await wait_for_pending_wakeups()
        assert not done.done
        set_mock_value(sis.mcs.acquiring, False)
        await done
        # Complete retains the final snapshot; future IOC changes cannot alter it.
        publish(sis, 0, [0], [0])
        (final,) = await pages(sis)
        assert set(await sis.describe_collect()) == {"sis-channel1", "sis-channel2"}
        assert set(final["data"]) == {"sis-channel1", "sis-channel2"}
        np.testing.assert_array_equal(
            final["data"]["sis-channel1"], [11, 22, 33, 16777217]
        )
        assert final["data"]["sis-channel1"].dtype == np.dtype(np.int32)
        assert await pages(sis) == []
        await sis.unstage()

    run(scenario())


@pytest.mark.parametrize(
    "count, channel1, channel2, nords, message",
    [
        (3, [11, 22, 33], [101, 102, 103], None, "channel 1"),
        (4, [11, 22, 33, 44], [101, 102, 103], None, "channel 2"),
        (4, [11, 22, 33, 44], [101, 102, 103, 104], (4, 3), "channel 2"),
        (5, [11, 22, 33, 44], [101, 102, 103, 104], None, "5 of 4 bins"),
    ],
)
def test_completion_rejects_missing_or_extra_bins(
    count, channel1, channel2, nords, message
):
    async def scenario():
        sis = await make_sis()
        await sis.prepare(info())
        await sis.kickoff()
        publish(sis, count, channel1, channel2, nords=nords)
        set_mock_value(sis.mcs.acquiring, False)
        with pytest.raises(RuntimeError, match=message):
            await sis.complete()
        await sis.unstage()

    run(scenario())


def test_completion_propagates_read_error_without_count_timeout():
    async def scenario():
        sis = await make_sis()
        await sis.prepare(info())
        await sis.kickoff()
        failure = OSError("MCA read failed")
        sis.mcs.mcas[2].data._connector.backend.get_reading = AsyncMock(
            side_effect=failure
        )
        publish(sis, 4, [11, 22, 33, 44], [101, 102, 103, 104])
        set_mock_value(sis.mcs.acquiring, False)
        with pytest.raises(OSError, match="MCA read failed") as caught:
            await sis.complete()
        assert caught.value is failure
        await sis.unstage()

    run(scenario())


@pytest.mark.parametrize("operation", ["stop", "unstage"])
def test_interruption_disarms_and_releases_pending_completion(operation):
    async def scenario():
        sis = await make_sis()
        await sis.prepare(info())
        await sis.kickoff()
        done = sis.complete()
        await wait_for_pending_wakeups()
        assert not done.done
        await getattr(sis, operation)()
        with pytest.raises(asyncio.CancelledError):
            await done
        assert not await sis.mcs.acquiring.get_value()
        assert sis.mcs.acquiring._cache is None
        assert sis.mcs.current_channel._cache is None
        await sis.stage()
        await sis.prepare(info(2))
        await sis.kickoff()
        publish(sis, 2, [5, 6], [7, 8])
        set_mock_value(sis.mcs.acquiring, False)
        await sis.complete()
        (page,) = await pages(sis)
        np.testing.assert_array_equal(page["data"]["sis-channel1"], [5, 6])
        await sis.unstage()

    run(scenario())


@pytest.mark.parametrize("capacity_owner", ["driver", "record"])
def test_insufficient_capacity_is_rejected_before_arming(capacity_owner):
    async def scenario():
        sis = await make_sis()
        capacity = (
            sis.mcs.max_channels if capacity_owner == "driver" else sis.mcs.mcas[2].nmax
        )
        set_mock_value(capacity, 3)
        with pytest.raises(ValueError, match="requested 4 bins, capacity is 3"):
            await sis.prepare(info())
        assert get_mock_put(sis.mcs.erase_start).call_count == 0
        assert not await sis.mcs.acquiring.get_value()
        await sis.unstage()

    run(scenario())


@pytest.mark.parametrize(
    "trigger_info, message",
    [
        (info(0), "must be finite"),
        (TriggerInfo(), "EXTERNAL_LEVEL"),
        (info(collections_per_event=2), "one exposure and one bin"),
        (info(exposures_per_collection=2), "one exposure and one bin"),
    ],
)
def test_unsupported_acquisition_contract_is_rejected_before_arming(
    trigger_info, message
):
    async def scenario():
        sis = await make_sis()
        with pytest.raises(ValueError, match=message):
            await sis.prepare(trigger_info)
        assert get_mock_put(sis.mcs.erase_start).call_count == 0
        await sis.unstage()

    run(scenario())


def test_missing_pulses_time_out_without_success():
    async def scenario():
        sis = await make_sis()
        await sis.prepare(info(exposure_timeout=0.01))
        await sis.kickoff()
        with pytest.raises(TimeoutError):
            await sis.complete()
        assert not await sis.mcs.acquiring.get_value()
        await sis.unstage()

    run(scenario())


def test_arm_failure_disarms_and_releases_subscriptions():
    async def scenario():
        sis = await make_sis()
        failure = OSError("arm command failed")

        def failing_arm(value):
            set_mock_value(sis.mcs.acquiring, True)
            raise failure

        callback_on_mock_put(sis.mcs.erase_start, failing_arm)
        with pytest.raises(OSError, match="arm command failed") as caught:
            await sis.prepare(info())
        assert caught.value is failure
        assert not await sis.mcs.acquiring.get_value()
        assert sis.mcs.acquiring._cache is None
        await sis.unstage()

    run(scenario())


def test_single_bin_keeps_zero_counts():
    async def scenario():
        sis = await make_sis()
        await sis.prepare(info(1))
        await sis.kickoff()
        publish(sis, 1, [0], [17])
        set_mock_value(sis.mcs.acquiring, False)
        await sis.complete()
        (page,) = await pages(sis)
        np.testing.assert_array_equal(page["data"]["sis-channel1"], [0])
        np.testing.assert_array_equal(page["data"]["sis-channel2"], [17])
        await sis.unstage()

    run(scenario())


def test_reprepare_cannot_erase_an_acquisition_with_uncollected_data():
    async def scenario():
        sis = await make_sis()
        await sis.prepare(info())
        with pytest.raises(RuntimeError, match="before preparing another acquisition"):
            await sis.prepare(info(2))
        await sis.kickoff()
        publish(sis, 4, [11, 22, 33, 44], [101, 102, 103, 104])
        set_mock_value(sis.mcs.acquiring, False)
        await sis.complete()
        with pytest.raises(RuntimeError, match="before preparing another acquisition"):
            await sis.prepare(info(2))
        (page,) = await pages(sis)
        np.testing.assert_array_equal(page["data"]["sis-channel1"], [11, 22, 33, 44])
        await sis.unstage()

    run(scenario())
