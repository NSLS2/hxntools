from __future__ import absolute_import

# imports to expose out to world
from .timepix import TimepixHDF5Handler

__all__ = ["TimepixHDF5Handler"]


def register(db):
    from .xspress3 import register
    register(db)
    from .timepix import register
    register(db)
    from .rasmi2 import register
    register(db)
    from .bulkdata import register
    register(db)
