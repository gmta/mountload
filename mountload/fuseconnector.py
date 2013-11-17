# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

from errno import ENOENT
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging

class FUSEConnector(LoggingMixIn, Operations):
    def __init__(self, controllerPool, isDebugMode):
        self.controllerPool = controllerPool

        # Setup logger
        loglevel = logging.DEBUG if isDebugMode else logging.WARNING
        self.log.setLevel(loglevel)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s - Thd %(thread)d - %(levelname)s - %(message)s'))
        sh.setLevel(loglevel)
        self.log.addHandler(sh)

    def _callControllerMethod(self, methodName, *args, **kwargs):
        """Acquires a Controller instance and performs a method call on it"""
        controller = self.controllerPool.acquireController()
        returnValue = getattr(controller, methodName)(*args, **kwargs)
        self.controllerPool.releaseController(controller)
        return returnValue

    def destroy(self, path):
        self.controllerPool.close()

    def getattr(self, path, fh=None):
        attr = self._callControllerMethod('getStatForPath', path)
        if attr is None:
            raise FuseOSError(ENOENT)
        return attr

    def read(self, path, size, offset, fh):
        return self._callControllerMethod('readData', path, offset, size)

    def readdir(self, path, fh):
        entries = ['.', '..']
        for entry in self._callControllerMethod('getEntriesInDirectory', path):
            entries.append(entry['basename'])
        return entries

    def readlink(self, path):
        return self._callControllerMethod('getSymlinkTarget', path)

    def startFUSE(self, mountpoint, isDaemonized=True, isMultiThreaded=False):
        """Starts FUSE using itself as the connector"""
        FUSE(self, mountpoint, foreground=not isDaemonized, nothreads=not isMultiThreaded)
