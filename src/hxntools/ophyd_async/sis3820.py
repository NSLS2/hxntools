"""Finite SIS3820 MCS acquisition through the EPICS mca module.

Counts are read once after acquisition completes and collected as scalar integer
EventPages in an independent stream. Acquiring=Done is the sequencer's
final-readout barrier.
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


class Sis3820McsIO(EpicsDevice):
    """SIS38XX controls and selected MCA records, without scalerRecord signals."""

    acquiring: A[SignalR[bool], PvSuffix("Acquiring")]
    current_channel: A[SignalR[int], PvSuffix("CurrentChannel")]
    max_channels: A[SignalR[int], PvSuffix("MaxChannels")]
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


class Sis3820Acquisition(FlyableLogic[TriggerInfo, TriggerInfo]):
    """Coordinate finite arming and one final readout."""

    def __init__(
        self,
        mcs: Sis3820McsIO,
        input_mode: Sis3820InputMode,
        input_polarity: Sis3820Polarity,
        count_on_start: bool,
    ) -> None:
        self.mcs = mcs
        self.input_mode = input_mode
        self.input_polarity = input_polarity
        self.count_on_start = count_on_start
        self._prepared = False
        self._completed = False
        self._snapshot: _Snapshot | None = None
        self._completion_task: asyncio.Task | None = None

    async def on_stage(self) -> None:
        await self.stop()
        self._prepared = False
        self._completed = False
        self._snapshot = None

    async def _set_acquiring(self, command: SignalW[bool], acquiring: bool) -> None:
        # Own the iterator so returning on a match releases its subscription.
        async with aclosing(
            observe_signals_value(self.mcs.acquiring, timeout=DEFAULT_TIMEOUT)
        ) as values:
            await anext(values)
            await command.set(True)
            if await self.mcs.acquiring.get_value(cached=False) == acquiring:
                return
            async for _, value in values:
                if value == acquiring:
                    return

    async def _stop_hardware(self) -> None:
        await self._set_acquiring(self.mcs.stop_all, False)

    async def _read_final(self, expected: int) -> _Snapshot:
        # Read the sequencer's barrier before the arrays, not HardwareAcquiring.
        acquiring = await self.mcs.acquiring.get_value(cached=False)
        count = await self.mcs.current_channel.get_value(cached=False)
        readings = await asyncio.gather(
            *(mca.data.read(cached=False) for mca in self.mcs.mcas.values())
        )
        nords = await asyncio.gather(
            *(mca.nord.get_value(cached=False) for mca in self.mcs.mcas.values())
        )
        values = {}
        timestamps = {}
        for (n, mca), reading, nord in zip(
            self.mcs.mcas.items(), readings, nords, strict=True
        ):
            value = reading[mca.data.name]
            values[n] = value["value"]
            timestamps[n] = value["timestamp"]
            if nord != expected or len(values[n]) != expected:
                raise RuntimeError(
                    f"SIS3820 channel {n} ended with NORD={nord}, "
                    f"array length={len(values[n])}; expected {expected} bins"
                )
        if acquiring or count != expected:
            raise RuntimeError(
                f"SIS3820 acquisition ended with {count} of {expected} bins "
                f"(Acquiring={acquiring})"
            )
        return _Snapshot(count, values, timestamps)

    async def on_prepare(self, value: TriggerInfo) -> TriggerInfo:
        if self._prepared:
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
        try:
            await self._set_acquiring(self.mcs.erase_start, True)
        except BaseException:
            await self.stop()
            raise
        self._prepared = True
        self._completed = False
        self._snapshot = None
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
            self._snapshot = await self._read_final(ctx.number_of_events)
            self._completed = True
        except Exception:
            await self._stop_hardware()
            raise
        finally:
            self._completion_task = None

    def next_page(self, name: str) -> PartialEventPage | None:
        if not self._completed:
            raise RuntimeError("Complete SIS3820 before collecting data")
        snapshot = self._snapshot
        if snapshot is None:
            return None
        timestamp = max(snapshot.timestamps.values())
        data = {f"{name}-channel{n}": values for n, values in snapshot.values.items()}
        timestamps = {
            f"{name}-channel{n}": [snapshot.timestamps[n]] * snapshot.count
            for n in snapshot.values
        }
        self._snapshot = None
        return {
            "time": [timestamp] * snapshot.count,
            "data": data,
            "timestamps": timestamps,
        }

    async def stop(self) -> None:
        try:
            task = self._completion_task
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        finally:
            await self._stop_hardware()


class Sis3820Mcs(StandardFlyable[TriggerInfo, TriggerInfo], StandardReadable):
    """SIS3820 flyer publishing integer counts in its own Bluesky stream.

    Each stage supports one finite EXTERNAL_LEVEL acquisition. Prepare erases
    and arms; kickoff does not touch hardware. Complete the acquisition before
    collecting one Event per externally latched bin.

    The external wiring must provide both next pulses and counting inhibit for
    the selected input mode/polarity. With CountOnStart enabled, inhibit must
    exclude the arm-to-first-exposure interval and time between exposures.
    Channel 1's reference-clock choice is recorded, not changed.

    Event timestamps are MCA record readout timestamps, not per-bin exposure
    timestamps. Align other detector/position streams by Bluesky Event sequence;
    the SIS does not reconstruct times or positions. Collect it separately from
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
        self._acquisition = Sis3820Acquisition(
            self.mcs, input_mode, input_polarity, count_on_start
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
            f"{self.name}-channel{n}": DataKey(
                source=mca.data.source,
                dtype="integer",
                dtype_numpy=np.dtype(np.int32).str,
                shape=[],
                units="counts",
            )
            for n, mca in self.mcs.mcas.items()
        }

    async def collect_pages(self) -> AsyncIterator[PartialEventPage]:
        if page := self._acquisition.next_page(self.name):
            yield page

    @AsyncStatus.wrap
    async def stop(self, success: bool = False) -> None:
        """Disarm and release any pending completion subscription."""
        await self.flyable_logic.stop()
