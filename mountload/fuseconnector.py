# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

from errno import ENOENT
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging

class FUSEConnector(LoggingMixIn, Operations):
    def __init__(self, mountload):
        self.mountload = mountload

        # Setup logger
        loglevel = logging.WARNING
        # loglevel = logging.DEBUG
        self.log.setLevel(loglevel)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s - Thd %(thread)d - %(levelname)s - %(message)s'))
        sh.setLevel(loglevel)
        self.log.addHandler(sh)

    def destroy(self, path):
        self.mountload.close()

    def getattr(self, path, fh=None):
        attr = self.mountload.getStatForPath(path)
        if attr is None:
            raise FuseOSError(ENOENT)
        return attr

    def read(self, path, size, offset, fh):
        return self.mountload.readData(path, offset, size)

    def readdir(self, path, fh):
        entries = ['.', '..']
        for entry in self.mountload.getEntriesInDirectory(path):
            entries.append(entry['basename'])
        return entries

    def readlink(self, path):
        return self.mountload.getSymlinkTarget(path)

    def startFUSE(self, mountpoint, isDaemonized=True, isMultiThreaded=False):
        """Starts FUSE using itself as the connector"""
        FUSE(self, mountpoint, foreground=not isDaemonized, nothreads=not isMultiThreaded)
