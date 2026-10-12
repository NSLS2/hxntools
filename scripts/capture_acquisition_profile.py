#!/usr/bin/env python3
"""Export existing PandA EPICS settings to ophyd-async's panda.yaml format.

The JSON config maps ophyd-async dotted signal paths to explicit ca:// or
pva:// readback PVs. No RunEngine, device startup, native PandA connection or
configuration writes are used. Capture while the configuration is stable.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from ophyd_async.core import YamlSettingsProvider
from ophyd_async.epics.core import epics_signal_r


async def capture_panda_settings(directory: Path, signal_pvs: dict[str, str]) -> Path:
    """Read configured PVs and write panda.yaml only after all reads succeed."""
    destination = directory / "panda.yaml"
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"Settings file already exists: {destination}")
    if not signal_pvs:
        raise ValueError("At least one PandA signal/PV mapping is required")
    for path, pv in signal_pvs.items():
        if (
            not path
            or not isinstance(pv, str)
            or not pv.startswith(("ca://", "pva://"))
            or not pv.split("://", 1)[1]
        ):
            raise ValueError(
                f"Expected a signal path and explicit ca:// or pva:// PV: {path!r}: {pv!r}"
            )
    values = {}
    for path, pv in signal_pvs.items():
        signal = epics_signal_r(None, pv, name=path)
        await signal.connect()
        values[path] = await signal.get_value()
    directory.mkdir(parents=True, exist_ok=True)
    await YamlSettingsProvider(directory).store("panda", values)
    return destination


def _unique_keys(pairs: list[tuple[str, str]]) -> dict[str, str]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate signal path: {key!r}")
        result[key] = value
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="JSON mapping of PandA signal paths to EPICS readback PVs",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        required=True,
        help="Directory in which to write panda.yaml",
    )
    args = parser.parse_args()
    signal_pvs = json.loads(args.config.read_text(), object_pairs_hook=_unique_keys)
    if not isinstance(signal_pvs, dict):
        raise ValueError("Config must map PandA signal paths to EPICS readback PVs")
    destination = asyncio.run(capture_panda_settings(args.directory, signal_pvs))
    print(f"Captured PandA settings: {destination}")


if __name__ == "__main__":
    main()
