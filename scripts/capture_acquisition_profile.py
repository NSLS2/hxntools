#!/usr/bin/env python3
"""Capture every PVI-exposed writable PandA setting in panda.yaml.

The script connects an ophyd-async HDFPanda through the PandABlocks IOC's PVI
interface, discovers its read-write signals, and reads them with store_settings.
It does not apply settings or write to the PandA. Capture while configuration is
stable.
"""

from __future__ import annotations

import argparse
from collections.abc import Generator
from pathlib import Path

from bluesky import RunEngine
from bluesky.utils import Msg
from ophyd_async.core import (
    StaticFilenameProvider,
    StaticPathProvider,
    YamlSettingsProvider,
)
from ophyd_async.fastcs.panda import HDFPanda
from ophyd_async.plan_stubs import ensure_connected, store_settings


def capture_panda_settings(
    directory: Path, panda: HDFPanda
) -> Generator[Msg, None, Path]:
    """Read every discovered PandA SignalRW and write panda.yaml."""
    destination = directory / "panda.yaml"
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"Settings file already exists: {destination}")
    directory.mkdir(parents=True, exist_ok=True)
    yield from store_settings(YamlSettingsProvider(directory), "panda", panda)
    return destination


def capture_from_prefix(directory: Path, prefix: str) -> Path:
    """Connect to a PandABlocks PVI prefix and capture its settings."""
    path_provider = StaticPathProvider(
        StaticFilenameProvider("capture"), directory.resolve()
    )
    panda = HDFPanda(prefix, path_provider, name="panda")

    def connect_and_capture() -> Generator[Msg, None, Path]:
        yield from ensure_connected(panda)
        return (yield from capture_panda_settings(directory, panda))

    result = RunEngine(call_returns_result=True)(connect_and_capture())
    return result.plan_result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prefix",
        required=True,
        help="PandABlocks IOC prefix whose PVI endpoint is <prefix>PVI",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        required=True,
        help="Directory in which to write panda.yaml",
    )
    args = parser.parse_args()
    destination = capture_from_prefix(args.directory, args.prefix)
    print(f"Captured PandA settings: {destination}")


if __name__ == "__main__":
    main()
