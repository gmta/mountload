# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

from errno import ENOENT
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging

class FUSEConnector(LoggingMixIn, Operations):
    def __init__(self, controllerPool):
        self.controllerPool = controllerPool

        # Setup logger
        loglevel = logging.WARNING
        # loglevel = logging.DEBUG
        self.log.setLevel(loglevel)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s - Thd %(thread)d - %(levelname)s - %(message)s'))
        sh.setLevel(loglevel)
        self.log.addHandler(sh)

    def destroy(self, path):
        self._getController().close()

    def getattr(self, path, fh=None):
        attr = self._getController().getStatForPath(path)
        if attr is None:
            raise FuseOSError(ENOENT)
        return attr

    def _getController(self):
        return self.controllerPool.getController()

    def read(self, path, size, offset, fh):
        return self._getController().readData(path, offset, size)

    def readdir(self, path, fh):
        entries = ['.', '..']
        for entry in self._getController().getEntriesInDirectory(path):
            entries.append(entry['basename'])
        return entries

    def readlink(self, path):
        return self._getController().getSymlinkTarget(path)

    def startFUSE(self, mountpoint, isDaemonized=True, isMultiThreaded=False):
        """Starts FUSE using itself as the connector"""
        FUSE(self, mountpoint, foreground=not isDaemonized, nothreads=not isMultiThreaded)
