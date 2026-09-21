from .hxn_handler import HXNHandlerBase


class BulkMerlin(HXNHandlerBase):
    HANDLER_NAME = 'MERLIN_HDF5_BULK'

    def __call__(self, **kwargs):
        ds = self._handle['entry/instrument/detector/data']
        ds.id.refresh()
        return ds[:,:,:]

    def dataset(self):
        return self._handle['entry/instrument/detector/data']

class BulkXSP(HXNHandlerBase):
    HANDLER_NAME = 'XSP3_BULK'

    def __call__(self, frame = None, channel = None):
        ds = self._handle['entry/instrument/detector/data']
        ds.id.refresh()
        if channel is None:
            return ds[:,:,:]
        else:
            return ds[:,channel-1,:].squeeze()

class ROIHDF5Handler(HXNHandlerBase):
    HANDLER_NAME = "ROI_HDF5_FLY"

    def __call__(self, *, det_elem):
        ds = self._handle[det_elem]
        ds.id.refresh()
        return ds[:]

    def close(self):
        self._handle.close()
        self._handle = None
        super().close()

class PandAHandlerHDF5(HXNHandlerBase):
    """The handler to read HDF5 files produced by PandABox."""
    HANDLER_NAME = "PANDA"

    specs = {"PANDA"}

    def __call__(self, field):
        ds = self._handle[f"/{field}"]
        return ds[:]

class DexelaHandlerHDF5(HXNHandlerBase):
    """The handler to read HDF5 files produced by PandABox."""
    HANDLER_NAME = "DEX_HDF5"

    def __call__(self, frame):
        ds = self._handle['entry/instrument/detector/data']
        ds.id.refresh()
        return ds[frame,:,:]



def register(db):
    db.reg.register_handler(BulkMerlin.HANDLER_NAME,
                            BulkMerlin, overwrite=True)
    db.reg.register_handler(BulkXSP.HANDLER_NAME,
                            BulkXSP, overwrite=True)
    db.reg.register_handler(ROIHDF5Handler.HANDLER_NAME,
                            ROIHDF5Handler, overwrite=True)
    db.reg.register_handler(PandAHandlerHDF5.HANDLER_NAME,
                            PandAHandlerHDF5, overwrite=True)
    db.reg.register_handler(DexelaHandlerHDF5.HANDLER_NAME,
                            DexelaHandlerHDF5, overwrite=True)
