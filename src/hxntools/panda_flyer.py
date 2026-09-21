import itertools
import os
import time
import time as ttime
import uuid
from collections import OrderedDict, deque
from datetime import datetime

import h5py
import numpy as np
from bluesky.utils import short_uid
from event_model import compose_resource
from ophyd import Device
from ophyd.areadetector.filestore_mixins import resource_factory
from ophyd.sim import NullStatus

from hxntools.handlers.rasmi2 import SISHDF5Handler


class ExportSISDataPanda:
    def __init__(self):
        self._fp = None
        self._filepath = None

    def open(self, filepath, mca_names, ion, panda):
        self.close()
        self._filepath = filepath
        self._fp = h5py.File(filepath, "w", libver="latest")

        print(f'{filepath = }')

        self._fp.swmr_mode = True

        self._ion = ion
        self._panda = panda
        self._mca_names = mca_names

        def create_ds(ds_name):
            ds = self._fp.create_dataset(ds_name, data=np.array([], dtype="f"), maxshape=(None,), dtype="f")

        for ds_name in self._mca_names:
            create_ds(ds_name)

        self._fp.flush()

    def close(self):
        if self._fp:
            self._fp.close()
            self._fp = None

    def __del__(self):
        self.close()

    def export(self):

        n_mcas = len(self._mca_names)

        mca_data = []
        for n in range(1, n_mcas + 1):
            mca = self._ion.mca_by_index[n].spectrum.get(timeout=5.0)
            mca_data.append(mca)
        
        #some problem here ROI data is saved correctly but correct len is 0:  AP 04/24/2026
        correct_length = int(self._panda.data.num_captured.get()/self._panda.position_supersample)
        # correct_length = int(self._panda.data.num_capture.get()/self._panda.position_supersample) trial on 4/26/2026
        print (f"{'='*30}")
        print(f' {correct_length = }')
        print (f"{'='*30}")

        for n in range(len(mca_data)):
            mca = mca_data[n]
            # print(f"Number of mca points: {len(mca)}")
            # mca = mca[1::2]
            if len(mca) != correct_length:
                print(f"Incorrect number of points ({len(mca)}) loaded from MCA{n + 1}: {correct_length} points are expected")
                if len(mca > correct_length):
                    mca = mca[:correct_length]
                else:
                    mca = np.append(mca, [1e10] * (correct_length - len(mca)))
            mca_data[n] = mca

        j = 0
        while self._panda.data.capture.get() == 1:
            print("Waiting for pandabox data...")
            ttime.sleep(0.1)
            j += 1
            if j > 10:
                print("PANDABOX IS BEHAVING BADLY CARRYING ON")
                break

        def add_data(ds_name, data):
            ds = self._fp[ds_name]
            n_ds = ds.shape[0]
            ds.resize((n_ds + len(data),))
            ds[n_ds:] = np.array(data)

        for n, name in enumerate(self._mca_names):
            add_data(name, np.asarray(mca_data[n]))

        self._fp.flush()


class ExportXpsROI:
    def __init__(self):
        self._fp = None
        self._filepath = None

    def open(self, filepath, xspress3):
        self.close()
        self._filepath = filepath
        self._fp = h5py.File(filepath, "w", libver="latest")

        self._fp.swmr_mode = True

        self._xsp = xspress3

        def create_ds(det_name):
            if not det_name in self._fp:
                self._fp.create_dataset(det_name, data=np.array([], dtype="f"), maxshape=(None,), dtype="f")

        for det_name in [roi.name for roi in self._xsp.enabled_rois]:
            create_ds(det_name)

        self._fp.flush()

    def close(self):
        if self._fp:
            self._fp.close()
            self._fp = None

    def __del__(self):
        self.close()

    def export(self, npoints):
        def add_data(det_name, data):
            ds = self._fp[det_name]
            if ds.size == 0:
                ds.resize((npoints,))
                ds[:len(data)] = np.array(data)
                ds[len(data):] = ds[len(data)-1]

        for roi in self._xsp.enabled_rois:
            if hasattr(roi,'settings'):
                add_data(roi.name, roi.settings.array_data.get())
            else:
                add_data(roi.name, roi.ts_total.get())
        self._fp.flush()


class HXNFlyerPanda(Device):
    """
    This is the PandaBox panda1 and panda2
    """

    @property
    def detectors(self):
        return tuple(self._dets)

    @detectors.setter
    def detectors(self, value):
        dets = tuple(value)
        # if not all([d.name in self.KNOWN_DETS for d in dets]):
        #     raise ValueError(
        #         f"One or more of {[d.name for d in dets]}"
        #         f"is not known to the panda. "
        #         f"The known detectors are {self.KNOWN_DETS})"
        #     )
        self._dets = dets

    @property
    def sclr(self):
        return self._sis

    def __init__(
        self,
        panda,
        dets,
        sclr,
        *,
        large_file_directory_root,
        large_file_directory_write_path,
        large_file_directory_read_path,
        motor=None,
        root_dir=None,
        n_scaler_mca=16,
        **kwargs,
    ):
        super().__init__("", parent=None, **kwargs)
        self.name = "PandaFlyer"
        self.LARGE_FILE_DIRECTORY_ROOT = large_file_directory_root
        self.LARGE_FILE_DIRECTORY_WRITE_PATH = large_file_directory_write_path
        self.LARGE_FILE_DIRECTORY_READ_PATH = large_file_directory_read_path
        if root_dir is None:
            root_dir = self.LARGE_FILE_DIRECTORY_ROOT
        self._mode = "idle"
        self._dets = dets
        self._sis = sclr
        self._root_dir = root_dir
        self._resource_document, self._datum_factory = None, None
        self._document_cache = deque()
        self._last_bulk = None

        self._point_counter = None
        self.frame_per_point = None
        # Number of raw PandA gates captured per *scan point*. Real callers
        # (flyscan_pd et al.) always set this before kickoff(); default to 1
        # (no oversampling) so the flyer is safe to use standalone.
        self.position_supersample = 1
        # Optional hook invoked as `callback(finished: bool)` from complete();
        # hxntools has no beamline live-plotting dependency of its own, so the
        # profile wires this to its plot-update function after construction.
        self.live_plot_callback = None
        self._n_scaler_mca = n_scaler_mca

        self.panda = panda
        self.motor = motor

        self._document_cache = []
        self._resource_document = None
        self._datum_factory = None

        if self._sis is not None:
            self._data_sis_exporter = ExportSISDataPanda()

        self._xsp_roi_exporter = None

        type_map = {"int32": "<i4", "float32": "<f4", "float64": "<f8"}

        self.fields = {
            "inenc1_val": {
                "value": "INENC1.VAL.Value",
                "dtype_str": type_map["int32"],
            },
            "inenc2_val": {
                "value": "INENC2.VAL.Value",
                "dtype_str": type_map["int32"],
            },
            "inenc3_val": {
                "value": "INENC3.VAL.Value",
                "dtype_str": type_map["int32"],
            },
            "inenc4_val": {
                "value": "INENC4.VAL.Value",
                "dtype_str": type_map["int32"],
            },
            "pcap_ts_trig": {
                "value": "PCAP.TS_TRIG.Value",
                "dtype_str": type_map["float64"],
            },
        }
        self.panda.data.hdf_directory.put_complete = True
        self.panda.data.hdf_file_name.put_complete = True

    def stage(self):
        super().stage()

    def unstage(self):
        self._point_counter = None
        if self._sis is not None:
            self._data_sis_exporter.close()
        if self._xsp_roi_exporter is not None:
            self._xsp_roi_exporter.close()
        super().unstage()

    def kickoff(self, *, num):
        """Kickoff the acquisition process."""
        # Prepare parameters:
        self._document_cache = deque()
        self._datum_docs = {}
        self._counter = itertools.count()
        self._point_counter = 0

        self.frame_per_point = int(num)
        # Prepare 'resource' factory.
        now = datetime.now()
        self.fl_path = self.LARGE_FILE_DIRECTORY_WRITE_PATH
        self.fl_name = f"panda_rbdata_{now.strftime('%Y%m%d_%H%M%S')}_{short_uid()}.h5"

        resource_path = self.fl_name
        self._resource_document, self._datum_factory, _ = compose_resource(
            start={"uid": "needed for compose_resource() but will be discarded"},
            spec="PANDA",
            root=self.fl_path,
            resource_path=resource_path,
            resource_kwargs={},
        )
        # now discard the start uid, a real one will be added later
        self._resource_document.pop("run_start")
        self._document_cache.append(("resource", self._resource_document))

        for key, value in self.fields.items():
            datum_document = self._datum_factory(datum_kwargs={"field": value["value"]})
            self._document_cache.append(("datum", datum_document))
            self._datum_docs[key] = datum_document


        ## Scaler
        if self._sis is not None:
            # Stop the SIS3820
            self._sis.stop_all.put(1)

        self.__filename_sis = "{}.h5".format(uuid.uuid4())
        self.__read_filepath_sis = os.path.join(
            self.LARGE_FILE_DIRECTORY_READ_PATH, self.__filename_sis
        )
        self.__write_filepath_sis = os.path.join(
            self.LARGE_FILE_DIRECTORY_WRITE_PATH, self.__filename_sis
        )

        self.__filestore_resource_sis, self._datum_factory_sis = resource_factory(
            SISHDF5Handler.HANDLER_NAME,
            root=self.LARGE_FILE_DIRECTORY_ROOT,
            resource_path=self.__read_filepath_sis,
            # `frame_per_point` here is the number of samples represented by a single
            # Datum, i.e. the true per-row length (see describe_collect() for the
            # matching descriptor shape) -- not the raw, supersampled PandA gate
            # count in `self.frame_per_point`.
            resource_kwargs={
                "frame_per_point": self.frame_per_point // self.position_supersample
            },
            path_semantics="posix",
        )

        resources = [self.__filestore_resource_sis]

        self._xsp_roi_exporter = None
        ## Xspress3 ROIs
        for d in self._dets:
            #print(f"{d = }")
            if d.name == 'xspress3' or d.name == 'xspress3_det2':

                self.xsp = d

                self.__filename_xsp_roi = "ROI0_{}.h5".format(uuid.uuid4())
                self.__read_filepath_xsp_roi = os.path.join(
                    self.LARGE_FILE_DIRECTORY_READ_PATH, self.__filename_xsp_roi
                )
                self.__write_filepath_xsp_roi = os.path.join(
                    self.LARGE_FILE_DIRECTORY_WRITE_PATH, self.__filename_xsp_roi
                )

                self._xsp_roi_exporter = ExportXpsROI()
                self._xsp_roi_exporter.open(
                    self. __write_filepath_xsp_roi, d
                )

                self.__filestore_resource_xsp_roi, self._datum_factory_xsp_roi = resource_factory(
                    'ROI_HDF5_FLY',
                    root=self.LARGE_FILE_DIRECTORY_ROOT,
                    resource_path=self.__read_filepath_xsp_roi,
                    # Same true per-row length as the SIS resource above (and
                    # describe_collect()'s declared shape) -- required so that
                    # BlueskyRunV3.validate() can reconcile the on-disk,
                    # single-column-per-ROI dataset shape with the declared
                    # [1, num_scan_points] structure.
                    resource_kwargs={
                        "frame_per_point": self.frame_per_point // self.position_supersample
                    },
                    path_semantics="posix",
                )


                resources.append(self.__filestore_resource_xsp_roi)

        # if self._sis:
        #     resources.append(self.__filestore_resource_sis)

        self._document_cache.extend(("resource", _) for _ in resources)

        if self._sis is not None:
            sis_mca_names = self._sis_mca_names()
            self._data_sis_exporter.open(
                self.__write_filepath_sis, mca_names=sis_mca_names, ion=self._sis, panda=self.panda
            )


        # Kickoff panda process:
        print(f"[Panda]Starting acquisition ...")

        self.panda.position_supersample = self.position_supersample

        self.panda.data.hdf_directory.set(self.fl_path).wait()
        self.panda.data.hdf_file_name.set(self.fl_name).wait()
        #self.panda.data.flush_period.set(1).wait()

        self.panda.data.capture_mode.set("FIRST_N").wait()
        self.panda.data.num_capture.set(self.frame_per_point).wait()

        self.panda.pcap.arm.set(1).wait()

        self.panda.data.capture.set(1).wait()

        print(f"[Panda]Panda kickoff complete ...")

        return NullStatus()

    def complete(self):
        print("[Panda]complete")
        """Wait for the acquisition process started in kickoff to complete."""
        # Wait until done
        timeout = 60
        counter = 0
        while (self.panda.data.capture.get() == 1) and (counter<timeout):
            time.sleep(0.1)
            counter+=1

        self.panda.pcap.arm.set(0).wait()
        self.panda.data.capture.put(0)

        for d in self._dets:
            d.stop(success=True)

        now = ttime.time()  # TODO: figure out how to get it from PandABox (maybe?)

        data_dict = {
            key: datum_doc["datum_id"] for key, datum_doc in self._datum_docs.items()
        }

        self._last_bulk = {
            "data": data_dict,
            "timestamps": {key: now for key in self._datum_docs},
            "time": now,
            "filled": {key: False for key in self._datum_docs},
        }

        if self._sis:
            sis_mca_names = self._sis_mca_names()
            sis_datum = []
            for name in sis_mca_names:
                sis_datum.append(self._datum_factory_sis({"column": name, "point_number": self._point_counter}))
            self._document_cache.extend(("datum", d) for d in sis_datum)

        # @timer_wrapper
        def get_sis_data():
            if self._sis is None:
                return
            self._data_sis_exporter.export()

        get_sis_data()

        if self._xsp_roi_exporter is not None:
            self._xsp_roi_exporter.export(int(self.frame_per_point/self.position_supersample))

            if self.live_plot_callback is not None:
                self.live_plot_callback(True)

            roi_datum = []
            for roi in self.xsp.enabled_rois:
                roi_datum.append(self._datum_factory_xsp_roi({"det_elem": roi.name}))
            self._last_bulk["data"].update({k: v["datum_id"] for k, v in zip([roi.name for roi in self.xsp.enabled_rois], roi_datum)})
            self._last_bulk["timestamps"].update({k: v["datum_id"] for k, v in zip([roi.name for roi in self.xsp.enabled_rois], roi_datum)})
            self._document_cache.extend(("datum", d) for d in roi_datum)

        for d in self._dets:
            if d.name != 'fs' and d.name != 'bshutter' and d.name != 'xspress3':
                #print (f"{'='*30}")
                #print (d)
                self._document_cache.extend(d.collect_asset_docs())
            if d.name == 'xspress3':
                doc_cnt = 0
                for doc in d.collect_asset_docs():
                    self._document_cache.append(doc)
                    doc_cnt += 1
                    if doc_cnt == 4:
                        break

        print("[Panda]collect data")

        if self._sis:
            self._last_bulk["data"].update({k: v["datum_id"] for k, v in zip(sis_mca_names, sis_datum)})
            self._last_bulk["timestamps"].update({k: v["datum_id"] for k, v in zip(sis_mca_names, sis_datum)})

        for d in self._dets:
            if d.name == 'merlin1' or d.name == 'merlin2':
                reading = d.read()
                self._last_bulk["data"].update(
                    {k: v["value"] for k, v in reading.items()}
                    )
                self._last_bulk["timestamps"].update(
                    {k: v["timestamp"] for k, v in reading.items()}
                    )
            if d.name.startswith('eiger'):
                reading = d.read()
                self._last_bulk["data"].update(
                    {k: v["value"] for k, v in reading.items()}
                    )
                self._last_bulk["timestamps"].update(
                    {k: v["timestamp"] for k, v in reading.items()}
                    )
            if d.name == 'xspress3':
                reading = d.read()
                self._last_bulk["data"].update(
                    {k: v["value"] for k, v in reading.items() if k.startswith('xspress3')}
                )
                self._last_bulk["timestamps"].update(
                    {k: v["timestamp"] for k, v in reading.items() if k.startswith('xspress3')}
                )
            if d.name == 'xspress3_det2':
                reading = d.read()
                self._last_bulk["data"].update(
                    {k: v["value"] for k, v in reading.items()}
                )
                self._last_bulk["timestamps"].update(
                    {k: time.time() for k, v in reading.items()}
                )

        return NullStatus()

    def describe_collect(self):
        """Describe the data structure."""
        return_dict = {"primary": OrderedDict()}
        desc = return_dict["primary"]

        ext_spec = "FileStore:"

        # `frame_per_point` (== `self._npts` previously) is the raw number of
        # PandA gates captured, which is `position_supersample` gates per
        # real scan point (see kickoff()/flyscan_pd()). The SIS scaler and
        # Xspress3 ROI exporters both collapse that oversampling before
        # writing their HDF5 files (ExportSISDataPanda.export() and
        # ExportXpsROI.export(), called with frame_per_point /
        # position_supersample in complete()), so the declared per-row
        # length for those keys must match the reduced, on-disk point count
        # -- not the raw supersampled gate count.
        num_scan_points = self.frame_per_point // self.position_supersample

        def _spec(source):
            return {
                "external": ext_spec,
                "dtype": "array",
                # ExportSISDataPanda/ExportXpsROI both hardcode a 32-bit float
                # HDF5 dataset (dtype="f") for every scaler/ROI channel; declare
                # it here so downstream consumers don't have to guess/default.
                "dtype_str": "<f4",
                "shape": [num_scan_points],
                "source": source,
            }

        for key, value in self.fields.items():
            desc.update(
                {
                    key: {
                        "source": "PANDA",
                        "dtype": "array",
                        "dtype_str": value["dtype_str"],
                        "shape": [
                            self.frame_per_point
                        ],  # TODO: figure out variable shape
                        "external": "FILESTORE:",
                    }
                }
            )

        for d in self._dets:
            if d.name == 'merlin1' or d.name == 'merlin2':
                desc.update(d.describe())
            if d.name.startswith('eiger'):
                desc.update(d.describe())
            if d.name == 'xspress3':
                desc.update([(k, v) for k,v in d.describe().items() if k.startswith('xspress3')])
            if d.name == 'xspress3_det2':
                desc.update(d.describe())

        if self._sis is not None:
            sis_mca_names = self._sis_mca_names()
            for n, name in enumerate(sis_mca_names):
                desc[name] = _spec(self._sis.mca_by_index[n + 1].spectrum.pvname)

        if self._xsp_roi_exporter is not None:
            for roi in self.xsp.enabled_rois:
                if hasattr(roi, 'settings'):
                    source = roi.settings.array_data.pvname
                else:
                    source = roi.ts_total.pvname
                desc[roi.name] = _spec(source)


        return return_dict

    def collect(self):
        yield self._last_bulk
        self._point_counter += 1

    def collect_asset_docs(self):
        """The method to collect resource/datum documents."""
        items = list(self._document_cache)
        self._document_cache.clear()
        yield from items

    def _sis_mca_names(self):
        n_mcas = self._n_scaler_mca
        return [getattr(self._sis.channels, f"chan{_}").name for _ in range(1, n_mcas + 1)]
