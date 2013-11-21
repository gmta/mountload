# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

from errno import ENOENT
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging

class FUSEConnector(LoggingMixIn, Operations):
    def __init__(self, controllerPool, isDebugMode):
        self.pool = controllerPool

        # Setup logger
        loglevel = logging.DEBUG if isDebugMode else logging.WARNING
        self.log.setLevel(loglevel)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s - Thd %(thread)d - %(levelname)s - %(message)s'))
        sh.setLevel(loglevel)
        self.log.addHandler(sh)

    def destroy(self, path):
        self.pool.close()

    def getattr(self, path, fh=None):
        with self.pool.acquire() as controller:
            attr = controller.getStatForPath(path)
        if attr is None:
            raise FuseOSError(ENOENT)
        return attr

    def read(self, path, size, offset, fh):
        with self.pool.acquire() as controller:
            return controller.readData(path, offset, size)

    def readdir(self, path, fh):
        with self.pool.acquire() as controller:
            return ['.', '..'] + [entry['basename'] for entry in controller.getEntriesInDirectory(path)]

    def readlink(self, path):
        with self.pool.acquire() as controller:
            return controller.getSymlinkTarget(path)

    def startFUSE(self, mountpoint, isMultiThreaded):
        """Starts FUSE using itself as the connector"""
        FUSE(self, mountpoint, foreground=True, nothreads=not isMultiThreaded)
