import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from bluesky import RunEngine
from ophyd_async.core import (
    StaticFilenameProvider,
    StaticPathProvider,
    YamlSettingsProvider,
    get_mock_put,
    set_mock_value,
)
from ophyd_async.fastcs.panda import HDFPanda
from ophyd_async.plan_stubs import ensure_connected

from scripts.capture_acquisition_profile import capture_panda_settings


class CapturePandaSettingsTest(unittest.TestCase):
    def test_captures_discovered_settings_without_putting(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            panda = HDFPanda(
                "TEST:",
                StaticPathProvider(
                    StaticFilenameProvider("capture"), directory.resolve()
                ),
                name="panda",
            )
            run_engine = RunEngine(call_returns_result=True)
            run_engine(ensure_connected(panda, mock=True))

            set_mock_value(panda.pcap.arm, True)
            set_mock_value(panda.data.capture, True)
            arm_put = get_mock_put(panda.pcap.arm)
            capture_put = get_mock_put(panda.data.capture)
            arm_put.reset_mock()
            capture_put.reset_mock()

            result = run_engine(capture_panda_settings(directory, panda))
            values = asyncio.run(YamlSettingsProvider(directory).retrieve("panda"))

            self.assertEqual(result.plan_result, directory / "panda.yaml")
            self.assertIs(values["pcap.arm"], True)
            self.assertIs(values["data.capture"], True)
            arm_put.assert_not_called()
            capture_put.assert_not_called()


if __name__ == "__main__":
    unittest.main()
