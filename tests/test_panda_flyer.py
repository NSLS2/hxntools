"""Integration tests for HXNFlyerPanda."""

import os
from types import SimpleNamespace

import bluesky.plan_stubs as bps
import h5py
import numpy as np
import pytest
from bluesky import RunEngine
from bluesky_tiled_plugins import TiledWriter
from bluesky_tiled_plugins.writing.tiled_writer import RunNormalizer

from hxntools.panda_flyer import HXNFlyerPanda

# Literal used by HXNFlyerPanda.kickoff() for the ROI resource spec.
ROI_SPEC = "ROI_HDF5_FLY"


def _alias_dataset(doc):
    """RunNormalizer patch: map the legacy `column`/`det_elem`/`field` datum_kwargs
    to `dataset`.

    The real SIS/ROI HDF5 files hold one flat dataset per channel/ROI name at
    the file root (see `ExportSISDataPanda.export()` / `ExportXpsROI.export()`),
    selected at read time via `column`/`det_elem`; the raw PandA capture file
    (spec "PANDA") is likewise one flat dataset per field, selected via `field`
    (see `kickoff()`'s `datum_kwargs={"field": value["value"]}`).
    `bluesky_tiled_plugins`'s generic HDF5 consolidator expects that selector
    under the `dataset` parameter; this mirrors the kind of normalizer patch a
    Tiled deployment supplies for these HXN-specific legacy handler specs.
    """
    dk = dict(doc.get("datum_kwargs", {}))
    for legacy_key in ("column", "det_elem", "field"):
        if legacy_key in dk:
            dk["dataset"] = dk.pop(legacy_key)
    doc["datum_kwargs"] = dk
    return doc


class _FakeSignal:
    """Minimal stand-in for an ophyd/EPICS-style signal: get/put/set(...).wait()."""

    def __init__(self, value=None, pvname=""):
        self.value = value
        self.pvname = pvname

    def get(self, **kwargs):
        return self.value

    def put(self, value, **kwargs):
        self.value = value

    def set(self, value, **kwargs):
        self.value = value
        return self

    def wait(self, **kwargs):
        return None


# PandA field PVs declared in HXNFlyerPanda.fields, with their true on-disk
# dtype (see the `type_map` used to build that dict in __init__).
PANDA_FIELD_DTYPES = {
    "INENC1.VAL.Value": "<i4",
    "INENC2.VAL.Value": "<i4",
    "INENC3.VAL.Value": "<i4",
    "INENC4.VAL.Value": "<i4",
    "PCAP.TS_TRIG.Value": "<f8",
}


class _FakeCaptureSignal(_FakeSignal):
    """Stand-in for the PandA `data.capture` PV.

    `get()` reports the capture as already finished, so `complete()`'s
    (blocking) wait loop exits at once. `set(1)` simulates the PandA
    hardware/IOC itself writing its raw capture HDF5 file -- one flat
    dataset per field, `frame_per_point` raw gates long -- at the
    directory/filename `kickoff()` configures on the sibling `hdf_directory`/
    `hdf_file_name`/`num_capture` signals just before arming.
    """

    def __init__(self, data_ns):
        super().__init__(value=0)
        self._data_ns = data_ns

    def get(self, **kwargs):
        return 0

    def set(self, value, **kwargs):
        self.value = value
        if value == 1:
            raw_count = int(self._data_ns.num_capture.value)
            path = os.path.join(
                self._data_ns.hdf_directory.value, self._data_ns.hdf_file_name.value
            )
            with h5py.File(path, "w") as fp:
                for field_name, dtype in PANDA_FIELD_DTYPES.items():
                    # HXNFlyerPanda.collect() yields exactly one Event for the
                    # whole flyer scan, so RunNormalizer declares each PandA
                    # field's structure with a leading singleton Event
                    # dimension: shape (1, frame_per_point). The PandA
                    # resource's compose_resource() call (unlike the SIS/ROI
                    # resource_factory() calls) does not set a `frame_per_point`
                    # resource_kwarg, so Tiled's validator does not reshape a
                    # flat on-disk array to match -- write the dataset already
                    # in the declared shape.
                    fp.create_dataset(
                        field_name, data=np.zeros((1, raw_count), dtype=dtype)
                    )
        return self


def _fake_panda():
    data = SimpleNamespace(
        hdf_directory=_FakeSignal(),
        hdf_file_name=_FakeSignal(),
        capture_mode=_FakeSignal(),
        num_capture=_FakeSignal(),
        num_captured=_FakeSignal(),
    )
    data.capture = _FakeCaptureSignal(data)
    return SimpleNamespace(data=data, pcap=SimpleNamespace(arm=_FakeSignal()))


def _fake_scaler(channel_names, real_points):
    channels = SimpleNamespace(
        **{
            f"chan{i}": SimpleNamespace(name=name)
            for i, name in enumerate(channel_names, start=1)
        }
    )
    mca_by_index = {
        i: SimpleNamespace(
            spectrum=_FakeSignal(
                np.arange(real_points, dtype="f8") + i * 1000, pvname=f"SCLR:mca{i}"
            )
        )
        for i in range(1, len(channel_names) + 1)
    }
    return SimpleNamespace(
        channels=channels, stop_all=_FakeSignal(), mca_by_index=mca_by_index
    )


def _fake_xspress3(roi_names, real_points):
    rois = [
        SimpleNamespace(
            name=name,
            settings=SimpleNamespace(
                array_data=_FakeSignal(
                    np.arange(real_points, dtype="f8") + i * 10, pvname=f"XSP:{name}"
                )
            ),
        )
        for i, name in enumerate(roi_names)
    ]
    return SimpleNamespace(
        name="xspress3",
        enabled_rois=rois,
        stop=lambda success=True: None,
        collect_asset_docs=lambda: iter(()),
        describe=lambda: {},
        read=lambda: {},
    )


@pytest.fixture(scope="module")
def tiled_context(tmp_path_factory):
    tiled_catalog = pytest.importorskip("tiled.catalog")
    tsa = pytest.importorskip("tiled.server.app")
    tc = pytest.importorskip("tiled.client")

    tmp_path = tmp_path_factory.mktemp("tiled_catalog")
    catalog = tiled_catalog.in_memory(
        writable_storage={
            "filesystem": str(tmp_path),
            "sql": f"duckdb:///{tmp_path}/test.db",
        },
        readable_storage=[str(tmp_path.parent)],
    )
    app = tsa.build_app(catalog)
    with tc.Context.from_app(app) as context:
        yield context


@pytest.fixture
def tiled_client(tiled_context):
    tc = pytest.importorskip("tiled.client")
    return tc.from_context(tiled_context)


def _primary_stream(run):
    node = run["streams"] if "streams" in run.keys() else run
    return node["primary"]


SCLR_CHANNELS = ["sclr1_ch1", "sclr1_ch2"]
ROI_NAMES = ["Det1_Fe", "Det1_Ni"]
REAL_POINTS = 20  # e.g. a 4x5 map


@pytest.mark.parametrize("position_supersample", [1, 10])
def test_flyer_declares_true_point_count(tiled_client, tmp_path, position_supersample):
    fake_panda = _fake_panda()
    fake_scaler = _fake_scaler(SCLR_CHANNELS, REAL_POINTS)
    fake_xspress3 = _fake_xspress3(ROI_NAMES, REAL_POINTS)

    flyer = HXNFlyerPanda(
        fake_panda,
        [fake_xspress3],
        fake_scaler,
        name="test_panda_flyer",
        # HXNFlyerPanda builds the SIS/ROI `resource_path` as an absolute path
        # already rooted at `large_file_directory_read_path` (see kickoff()'s
        # `os.path.join(self.LARGE_FILE_DIRECTORY_READ_PATH, filename)`), so
        # `root` must stay empty here -- otherwise bluesky_tiled_plugins's
        # root+resource_path URI composition would prefix the already-rooted
        # path a second time.
        large_file_directory_root="",
        large_file_directory_write_path=str(tmp_path),
        large_file_directory_read_path=str(tmp_path),
        n_scaler_mca=len(SCLR_CHANNELS),
    )
    flyer.position_supersample = position_supersample
    raw_count = REAL_POINTS * position_supersample

    writer = TiledWriter(
        tiled_client,
        normalizer=RunNormalizer,
        patches={"datum": _alias_dataset},
    )
    RE = RunEngine({})
    RE.subscribe(writer)

    def plan():
        yield from bps.open_run()
        yield from bps.kickoff(flyer, num=raw_count, wait=True)
        # Simulate the PandA hardware having captured exactly the requested
        # number of raw gates (ExportSISDataPanda.export() reads this back).
        fake_panda.data.num_captured.value = raw_count
        yield from bps.complete(flyer, wait=True)
        yield from bps.collect(flyer)
        yield from bps.close_run()

    (uid,) = RE(plan())
    primary = _primary_stream(tiled_client[uid])

    for key in SCLR_CHANNELS + ROI_NAMES:
        node = primary[key]
        assert node.structure().shape == (1, REAL_POINTS)
        assert node.read().shape == (1, REAL_POINTS)

    assert tiled_client[uid].validate(
        fix_errors=False, raise_on_error=True, write_notes=False
    )
