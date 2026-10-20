"""Finite SIS3820 MCS acquisition through the EPICS mca module.

Counts are collected as scalar integer EventPages in an independent stream.
Live collection requests fresh MCA readout and waits for its completion before
publishing counts. Acquiring=Done is the sequencer's final-readout barrier.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import aclosing, suppress
from dataclasses import dataclass
from functools import cached_property
from typing import Annotated as A

import numpy as np
from bluesky.protocols import PartialEventPage
from event_model import DataKey
from ophyd_async.core import (
    DEFAULT_TIMEOUT,
    Array1D,
    AsyncStatus,
    DeviceVector,
    DetectorTrigger,
    FlyableLogic,
    SignalR,
    SignalRW,
    SignalW,
    StandardFlyable,
    StandardReadable,
    StandardReadableFormat,
    StrictEnum,
    SubsetEnum,
    TriggerInfo,
    TriggerableCommand,
    YesNo,
    observe_signals_value,
    soft_signal_r_and_setter,
)
from ophyd_async.epics.core import EpicsDevice, PvSuffix, epics_signal_w


class Sis3820ChannelAdvance(StrictEnum):
    INTERNAL = "Internal"
    EXTERNAL = "External"


class Sis3820InputMode(SubsetEnum):
    """Input modes with external next-pulse and counting-inhibit inputs."""

    MODE_2 = "Mode 2"
    MODE_3 = "Mode 3"
    MODE_6 = "Mode 6"


class Sis3820Polarity(StrictEnum):
    NORMAL = "Normal"
    INVERTED = "Inverted"


class Sis3820Channel1Source(StrictEnum):
    INTERNAL = "Int. clock"
    EXTERNAL = "External"


class Sis3820McaIO(EpicsDevice):
    """One MCA record; NORD is distinct from the allocated array capacity."""

    data: A[SignalR[Array1D[np.int32]], PvSuffix(".VAL")]
    nord: A[SignalR[int], PvSuffix(".NORD")]
    nmax: A[SignalR[int], PvSuffix(".NMAX")]
    nuse: A[SignalRW[int], PvSuffix(".NUSE")]
    description: A[SignalR[str], PvSuffix(".DESC")]
    reading: A[SignalR[bool], PvSuffix(".RDNG")]
    read_time: A[SignalR[float], PvSuffix(".RTIM")]
    erase: A[TriggerableCommand, PvSuffix(".ERAS")]

    def __init__(self, prefix: str, name: str = "") -> None:
        # The mca record can defer put-callback until acquisition finishes.
        self.read_array = epics_signal_w(bool, f"{prefix}.READ", wait=False)
        super().__init__(prefix, name=name)


class Sis3820McsIO(EpicsDevice):
    """SIS38XX controls and selected MCA records, without scalerRecord signals."""

    acquiring: A[SignalR[bool], PvSuffix("Acquiring")]
    current_channel: A[SignalR[int], PvSuffix("CurrentChannel")]
    max_channels: A[SignalR[int], PvSuffix("MaxChannels")t ]
    nuse_all: A[SignalRW[int], PvSuffix("NuseAll")]
    channel_advance: A[SignalRW[Sis3820ChannelAdvance], PvSuffix("ChannelAdvance")]
    input_mode: A[SignalRW[Sis3820InputMode], PvSuffix("InputMode")]
    input_polarity: A[SignalRW[Sis3820Polarity], PvSuffix("InputPolarity")]
    count_on_start: A[SignalRW[YesNo], PvSuffix("CountOnStart")]
    channel1_source: A[SignalRW[Sis3820Channel1Source], PvSuffix("Channel1Source")]
    prescale: A[SignalRW[int], PvSuffix("Prescale")]
    preset_real: A[SignalRW[float], PvSuffix("PresetReal")]

    def __init__(
        self, prefix: str, *, channel_numbers: Sequence[int], name: str = ""
    ) -> None:
        # Put-callback can wait for the whole acquisition via the busy record.
        # Arm/stop completion is instead observed on Acquiring.
        self.erase_start = epics_signal_w(bool, f"{prefix}EraseStart", wait=False)
        self.stop_all = epics_signal_w(bool, f"{prefix}StopAll", wait=False)
        self.mcas = DeviceVector(
            {n: Sis3820McaIO(f"{prefix}Mca:{n}") for n in channel_numbers}
        )
        super().__init__(prefix, name=name)


@dataclass
class _Snapshot:
    count: int
    values: dict[int, Array1D[np.int32]]
    timestamps: dict[int, float]


class Sis3820Readout:
    """Own a single acquisition's valid prefix and collection cursor."""

    def __init__(self, mcs: Sis3820McsIO) -> None:
        self.mcs = mcs
        self.expected: int | None = None
        self.collected = 0
        self.final_snapshot: _Snapshot | None = None
        self.failure: Exception | None = None
        self.lock = asyncio.Lock()

    def reset(self) -> None:
        self.expected = None
        self.collected = 0
        self.final_snapshot = None
        self.failure = None

    async def _refresh(self, mca: Sis3820McaIO) -> None:
        # RTIM identifies a new read request within this record; it is not an
        # acquisition timestamp or a cross-record clock comparison. RDNG=False
        # then means that request's asyn callback has installed the array.
        async with aclosing(
            observe_signals_value(mca.read_time, mca.reading, timeout=DEFAULT_TIMEOUT)
        ) as updates:
            await anext(updates)
            previous = await mca.read_time.get_value(cached=False)
            await mca.read_array.set(True)
            async for signal, value in updates:
                if signal is mca.read_time and value != previous:
                    break
            if not await mca.reading.get_value(cached=False):
                return
            async for _, _ in updates:
                if not await mca.reading.get_value(cached=False):
                    return

    async def snapshot(self, *, final: bool = False) -> _Snapshot:
        if self.expected is None:
            raise RuntimeError("Prepare SIS3820 before collecting data")
        # Read the sequencer's barrier before the arrays, not HardwareAcquiring.
        acquiring = await self.mcs.acquiring.get_value(cached=False)
        final = final or not acquiring
        count = await self.mcs.current_channel.get_value(cached=False)
        if acquiring:
            await asyncio.gather(*(self._refresh(mca) for mca in self.mcs.mcas.values()))
        readings = await asyncio.gather(
            *(mca.data.read(cached=False) for mca in self.mcs.mcas.values())
        )
        nords = await asyncio.gather(
            *(mca.nord.get_value(cached=False) for mca in self.mcs.mcas.values())
        )
        values = {}
        timestamps = {}
        for (n, mca), reading, nord in zip(self.mcs.mcas.items(), readings, nords, strict=True):
            value = reading[mca.data.name]
            values[n] = value["value"]
            timestamps[n] = value["timestamp"]
            if final and (nord != self.expected or len(values[n]) != self.expected):
                raise RuntimeError(
                    f"SIS3820 channel {n} ended with NORD={nord}, "
                    f"array length={len(values[n])}; expected {self.expected} bins"
                )
        if final and (acquiring or count != self.expected):
            raise RuntimeError(
                f"SIS3820 acquisition ended with {count} of {self.expected} bins "
                f"(Acquiring={acquiring})"
            )
        # The mca record/driver can return one dummy element at zero bins.
        # CurrentChannel is nextChan_ in drvSIS38XX, a completed-bin count.
        valid = min(count, *nords, *(len(value) for value in values.values()))
        if valid < self.collected or count > self.expected:
            raise RuntimeError(
                f"SIS3820 bin count changed unexpectedly: completed={count}, "
                f"valid={valid}, collected={self.collected}, expected={self.expected}"
            )
        return _Snapshot(valid, values, timestamps)

    async def finish(self) -> None:
        async with self.lock:
            self.final_snapshot = await self.snapshot(final=True)

    async def next_page(self, name: str) -> PartialEventPage | None:
        async with self.lock:
            if self.failure is not None:
                raise self.failure
            snapshot = self.final_snapshot or await self.snapshot()
            start, stop = self.collected, snapshot.count
            if start == stop:
                return None
            length = stop - start
            timestamp = max(snapshot.timestamps.values())
            data = {f"{name}-point_index": np.arange(start, stop, dtype=np.int64)}
            timestamps = {f"{name}-point_index": [timestamp] * length}
            for n, values in snapshot.values.items():
                key = f"{name}-channel{n}"
                data[key] = values[start:stop]
                timestamps[key] = [snapshot.timestamps[n]] * length
            self.collected = stop
            return {"time": [timestamp] * length, "data": data, "timestamps": timestamps}


class Sis3820Acquisition(FlyableLogic[TriggerInfo, TriggerInfo]):
    """Coordinate finite arming and final readout; publication does not arm."""

    def __init__(
        self,
        mcs: Sis3820McsIO,
        readout: Sis3820Readout,
        input_mode: Sis3820InputMode,
        input_polarity: Sis3820Polarity,
        count_on_start: bool,
    ) -> None:
        self.mcs = mcs
        self.readout = readout
        self.input_mode = input_mode
        self.input_polarity = input_polarity
        self.count_on_start = count_on_start
        self._completion_task: asyncio.Task | None = None

    async def on_stage(self) -> None:
        await self.stop()
        self.readout.reset()

    async def _set_acquiring(self, command: SignalW[bool], acquiring: bool) -> None:
        # Own the iterator so returning on a match releases its subscription.
        async with aclosing(
            observe_signals_value(self.mcs.acquiring, timeout=DEFAULT_TIMEOUT)
        ) as values:
            await anext(values)
            await command.set(True)
            current = await self.mcs.acquiring.get_value(cached=False)
            if current == acquiring:
                return
            async for _, value in values:
                if value == acquiring:
                    return

    async def _erase_record(self, mca: Sis3820McaIO) -> None:
        async with aclosing(
            observe_signals_value(mca.nord, timeout=DEFAULT_TIMEOUT)
        ) as values:
            await anext(values)
            await mca.erase.trigger()
            if await mca.nord.get_value(cached=False) == 0:
                return
            async for _, value in values:
                if value == 0:
                    return

    async def on_prepare(self, value: TriggerInfo) -> TriggerInfo:
        if self.readout.expected is not None:
            raise RuntimeError("Stage SIS3820 before preparing another acquisition")
        if value.trigger is not DetectorTrigger.EXTERNAL_LEVEL:
            raise ValueError("SIS3820 supports EXTERNAL_LEVEL triggering only")
        if value.number_of_events == 0:
            raise ValueError("SIS3820 MCS acquisition must be finite")
        if value.collections_per_event != 1 or value.exposures_per_collection != 1:
            raise ValueError("SIS3820 requires one exposure and one bin per event")
        count = value.number_of_events
        capacities = await asyncio.gather(
            self.mcs.max_channels.get_value(),
            *(mca.nmax.get_value() for mca in self.mcs.mcas.values()),
        )
        if count > min(capacities):
            raise ValueError(
                f"SIS3820 requested {count} bins, capacity is {min(capacities)}"
            )
        await self.stop()
        await asyncio.gather(
            self.mcs.channel_advance.set(Sis3820ChannelAdvance.EXTERNAL),
            self.mcs.input_mode.set(self.input_mode),
            self.mcs.input_polarity.set(self.input_polarity),
            self.mcs.count_on_start.set(YesNo.YES if self.count_on_start else YesNo.NO),
            self.mcs.prescale.set(1),
            self.mcs.preset_real.set(0.0),
            *(mca.nuse.set(count) for mca in self.mcs.mcas.values()),
        )
        await self.mcs.nuse_all.set(count)
        # Clear each record's NORD and buffer explicitly. EraseStart only erases
        # the driver's buffer, so old MCA arrays otherwise survive until ReadAll.
        await asyncio.gather(*(self._erase_record(mca) for mca in self.mcs.mcas.values()))
        self.readout.expected = count
        try:
            await self._set_acquiring(self.mcs.erase_start, True)
        except BaseException:
            await self.stop()
            raise
        return value

    async def on_kickoff(self, ctx: TriggerInfo) -> TriggerInfo:
        # The external source must remain quiet until every detector is armed.
        # Prepare already armed the SIS; never erase/re-arm at kickoff.
        return ctx

    async def on_complete(self, ctx: TriggerInfo) -> None:
        self._completion_task = asyncio.current_task()
        try:
            async with aclosing(
                observe_signals_value(
                    self.mcs.acquiring,
                    self.mcs.current_channel,
                    timeout=ctx.exposure_timeout,
                )
            ) as updates:
                async for signal, value in updates:
                    if signal is self.mcs.acquiring and not value:
                        break
            await self.readout.finish()
        except Exception as error:
            self.readout.failure = error
            await self._set_acquiring(self.mcs.stop_all, False)
            raise
        finally:
            self._completion_task = None

    async def stop(self) -> None:
        try:
            task = self._completion_task
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        finally:
            await self._set_acquiring(self.mcs.stop_all, False)


class Sis3820Mcs(StandardFlyable[TriggerInfo, TriggerInfo], StandardReadable):
    """SIS3820 flyer publishing integer counts in its own Bluesky stream.

    Each stage supports one finite EXTERNAL_LEVEL acquisition. Prepare erases
    and arms; kickoff does not touch hardware. One externally latched bin maps
    to one Event, identified by a zero-based ``<name>-point_index``.

    The external wiring must provide both next pulses and counting inhibit for
    the selected input mode/polarity. With CountOnStart enabled, inhibit must
    exclude the arm-to-first-exposure interval and time between exposures.
    Channel 1's reference-clock choice is recorded, not changed.

    Event timestamps are MCA record readout timestamps, not per-bin exposure
    timestamps. Align other detector/position streams by point index; the SIS
    does not reconstruct times or positions. Collect it separately from
    file-writing detectors, using ``declare_stream(sis, collect=True, ...)``.
    """

    def __init__(
        self,
        prefix: str,
        *,
        channel_numbers: Sequence[int] = range(1, 33),
        input_mode: Sis3820InputMode = Sis3820InputMode.MODE_2,
        input_polarity: Sis3820Polarity = Sis3820Polarity.NORMAL,
        count_on_start: bool = True,
        hinted_channels: Sequence[int] = (),
        name: str = "",
    ) -> None:
        if not channel_numbers:
            raise ValueError("Select at least one SIS3820 input channel")
        self.mcs = Sis3820McsIO(prefix, channel_numbers=channel_numbers)
        self.channel_numbers, _ = soft_signal_r_and_setter(
            Array1D[np.int32], np.asarray(channel_numbers, dtype=np.int32)
        )
        self._readout = Sis3820Readout(self.mcs)
        self._acquisition = Sis3820Acquisition(
            self.mcs, self._readout, input_mode, input_polarity, count_on_start
        )
        self._hinted_channels = tuple(hinted_channels)
        self.add_readables(
            [
                self.channel_numbers,
                self.mcs.nuse_all,
                self.mcs.channel_advance,
                self.mcs.input_mode,
                self.mcs.input_polarity,
                self.mcs.count_on_start,
                self.mcs.channel1_source,
                self.mcs.prescale,
                self.mcs.preset_real,
                *(mca.description for mca in self.mcs.mcas.values()),
            ],
            StandardReadableFormat.CONFIG_SIGNAL,
        )
        super().__init__(name=name)

    @cached_property
    def flyable_logic(self) -> Sis3820Acquisition:
        return self._acquisition

    @property
    def hints(self) -> dict:
        return {"fields": [f"{self.name}-channel{n}" for n in self._hinted_channels]}

    async def describe_collect(self) -> dict[str, DataKey]:
        return {
            f"{self.name}-point_index": DataKey(
                source=self.mcs.current_channel.source,
                dtype="integer",
                dtype_numpy=np.dtype(np.int64).str,
                shape=[],
            ),
            **{
                f"{self.name}-channel{n}": DataKey(
                    source=mca.data.source,
                    dtype="integer",
                    dtype_numpy=np.dtype(np.int32).str,
                    shape=[],
                    units="counts",
                )
                for n, mca in self.mcs.mcas.items()
            },
        }

    async def collect_pages(self) -> AsyncIterator[PartialEventPage]:
        if page := await self._readout.next_page(self.name):
            yield page

    @AsyncStatus.wrap
    async def stop(self, success: bool = False) -> None:
        """Disarm and release any pending completion subscription."""
        await self.flyable_logic.stop()
