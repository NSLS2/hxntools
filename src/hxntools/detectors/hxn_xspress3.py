from collections import OrderedDict, namedtuple
import logging
import time
import numpy as np

from ophyd import (Component as Cpt, Signal)
from ophyd.areadetector.plugins import PluginBase
from ophyd.status import DeviceStatus
from ophyd.device import (BlueskyInterface, Staged)

from nslsii.detectors.xspress3 import (Xspress3Detector,
                                       Xspress3FileStore)
from .trigger_mixins import HxnModalBase

import pandas as pd

logger = logging.getLogger(__name__)


class HxnXspressTrigger(HxnModalBase, BlueskyInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._status = None
        self._acquisition_signal = self.settings.acquire
        self._abs_trigger_count = 0

    def unstage(self):
        ret = super().unstage()
        try:
            self._acquisition_signal.clear_sub(self._acquire_changed)
        except KeyError:
            pass

        return ret

    def _acquire_changed(self, value=None, old_value=None, **kwargs):
        "This is called when the 'acquire' signal changes."
        if self._status is None:
            return
        if (old_value == 1) and (value == 0):
            # Negative-going edge means an acquisition just finished.
            self._status._finished()

    def mode_internal(self):
        self._abs_trigger_count = 0
        self.stage_sigs[self.external_trig] = False
        self.stage_sigs[self.settings.acquire_time] = self.count_time.get()
        self._acquisition_signal.subscribe(self._acquire_changed)

    def mode_external(self):
        ms = self.mode_settings
        # NOTE: these are used in Xspress3Filestore.stage
        self.stage_sigs[self.external_trig] = True
        self.stage_sigs[self.total_points] = ms.total_points.get()
        self.stage_sigs[self.spectra_per_point] = 1
        self.stage_sigs[self.settings.acquire_time] = 0.001

        # NOTE: these are taken care of in Xspress3Filestore
        # self.stage_sigs[self.settings.trigger_mode] = 'TTL Veto Only'
        # self.stage_sigs[self.settings.num_images] = total_capture

    def _dispatch_channels(self, trigger_time):
        channels = self._channels.values()
        for sn in self.read_attrs:
            ch = getattr(self, sn)
            if ch in channels:
                self.generate_datum(ch.name, trigger_time,{})
        self._abs_trigger_count += 1

    def trigger_internal(self):
        if self._staged != Staged.yes:
            raise RuntimeError("not staged")
        self._status = DeviceStatus(self)
        # self.settings.erase.put(1)
        self._acquisition_signal.put(1, wait=True)
        self._dispatch_channels(trigger_time=time.time())
        return self._status

    def trigger_external(self):
        if self._staged != Staged.yes:
            raise RuntimeError("not staged")

        self._status = DeviceStatus(self)
        self._status._finished()
        if self.mode_settings.scan_type.get() != 'fly':
            self._dispatch_channels(trigger_time=time.time())
            # fly-scans take care of dispatching on their own

        return self._status

    def stage(self):
        staged = super().stage()
        mode = self.mode_settings.mode.get()
        if mode == 'external':
            # In external triggering mode, the devices is only triggered once
            # at stage
            self.settings.erase.put(1, wait=True)
            self._acquisition_signal.put(1, wait=False)
        return staged


class HxnXspress3DetectorBase(HxnXspressTrigger, Xspress3Detector):
    roi_data = Cpt(PluginBase, 'ROIDATA:')
    flyer_timestamps = Cpt(Signal)

    @property
    def hdf5_filename(self):
        return self.hdf5._fn

    def describe_collect(self):
        desc = Xspress3FileStore.describe(self.hdf5)

        for roi in self.enabled_rois:
            desc[roi.name] = dict(source='FILE:TBD', shape=[], dtype='number')

        return [desc]

    def bulk_read(self, timestamps=None):
        # TODO not compatible with collect() just yet due to the values
        #      returned
        if timestamps is None:
            timestamps = self.flyer_timestamps.get()

        if timestamps is None:
            raise ValueError('Timestamps must be set first')

        channels = self.channels
        count = len(timestamps)
        if count == 0:
            return {}

        ch_uids = {}
        for ch in channels:
            ch_uids[ch] = self.hdf5._reg.bulk_register_datum_table(
                self.hdf5._filestore_res, pd.DataFrame({'frame': range(count),
                                                        'channel': ch}))

        return OrderedDict((self.hdf5.mds_keys[ch], ch_uids[ch])
                           for ch in channels)

    def fly_collect_rois(self, rois=None, *, ignore_get_failures=True):
        '''Read ROI data from the PVs

        Parameters
        ----------
        rois : sequence of Xspress3ROI instances, optional
            If unspecified, uses all currently enabled ROIs
        ignore_get_failures : bool, optional
            Ignore pyepics-related failures - will
        '''
        if rois is None:
            rois = self.enabled_rois

        num_points = self.settings.num_images.get()
        RoiTuple = namedtuple('Xspress3ROITuple',
                              ['bin_low', 'bin_high',
                               'ev_low', 'ev_high',
                               'value', 'value_sum',
                               'enable'])

        for roi in self.enabled_rois:
            try:
                roi_data = roi.settings.array_data.get(count=num_points,
                                                       use_monitor=False)
            except Exception as ex:
                logger.error('Failed to get ROI data', exc_info=ex)
                if not ignore_get_failures:
                    raise
                roi_data = np.zeros(num_points)

            try:
                roi_data = roi_data[:num_points]
            except TypeError as ex:
                logger.error('Failed to get ROI data', exc_info=ex)
                if not ignore_get_failures:
                    raise
                roi_data = np.zeros(num_points)

            roi_info = RoiTuple(bin_low=roi.bin_low.get(),
                                bin_high=roi.bin_high.get(),
                                ev_low=roi.ev_low.get(),
                                ev_high=roi.ev_high.get(),
                                value=roi_data,
                                value_sum=None,
                                enable=roi.enable.get())

            yield roi.name, roi_info

    def stop(self, success=False):
        super().stop(success=success)

        logger.info('Ensuring detector %r capture stopped...',
                    self.name)
        self.settings.acquire.set(0).wait()
        self.hdf5.stop(success=success)
        logger.info('... detector %r ok', self.name)
