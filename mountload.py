#!/usr/bin/env python

import logging
import os.path
import stat

from errno import ENOENT
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from mountload.metadata import MountLoadMetaData
from mountload.source import MountLoadSource
from mountload.target import MountLoadTarget
from os import getgid, getuid
from sys import argv
from time import time

class MountLoad(LoggingMixIn, Operations):
    def __init__(self, sourceURI, targetDirectory):
        self.gid = getgid()
        self.uid = getuid()

        # Init source, target and metadata
        self.source = MountLoadSource(sourceURI)
        self.target = MountLoadTarget(targetDirectory)
        self.metadata = MountLoadMetaData(self.target.getDBPath())

        # Store and check source URI
        knownSourceURI = self.metadata.getConfigString('sourceURI')
        if knownSourceURI is None:
            self.metadata.setConfig('sourceURI', sourceURI)
        elif knownSourceURI != sourceURI:
            raise RuntimeError('Invalid source URI for this metadata')

        # Initialize root directory
        self.metadata.begin()
        if self._getPath('/') is None:
            self._registerPath('/', self.source.getEntry('/'))
        self.metadata.commit()

    def close(self):
        self.metadata.close()
        self.source.close()

    def _downloadFileData(self, pathInfo, offset, size):
        # Read data from source
        path = pathInfo['dirname'] + pathInfo['basename']
        data = self.source.readData(path, offset, size)

        # Write data to target
        self.target.writeData(path, offset, data)

        # Remove the remote segments we've overwritten
        pathId = pathInfo['pathId']
        self.metadata.begin()
        self.metadata.removeRemoteSegments(pathId, offset, offset + size - 1)

        # If all remote segments have been downloaded, we mark the file as synced
        if len(self.metadata.getRemoteSegments(pathId)) == 0:
            self.metadata.setPathSynced(pathId)
        self.metadata.commit()

        # Return the data
        return data

    def getEntriesInDirectory(self, dirpath):
        # Determine directory
        pathInfo = self._getPath(dirpath)
        if pathInfo is None:
            raise RuntimeError('Unknown path')
        if dirpath != '/':
            dirpath += '/'

        # Download all the entries in the directory if not synced
        if not pathInfo['isSynced']:
            self.metadata.begin()
            for entry in self.source.getDirectoryEntries(dirpath):
                if not self.metadata.getPath(dirpath, entry.filename):  # Entry can already exist
                    entryPath = dirpath + entry.filename
                    self._registerPath(entryPath, entry)
            self.metadata.setPathSynced(pathInfo['pathId'])
            self.metadata.commit()

        # Return subpaths
        return self.metadata.getSubPaths(dirpath)

    def _getPath(self, path):
        path = os.path.normpath(path)
        dirname, basename = MountLoad._splitPath(path)
        pathInfo = self.metadata.getPath(dirname, basename)

        # If no path was found, recursively check parent directory for sync
        if (pathInfo is None) and (path != '/'):
            parentDirInfo = self._getPath(os.path.dirname(path))
            if parentDirInfo is None:   # Parent directory doesn't exist, so this path can't exist either
                return None
            if parentDirInfo['isSynced']:   # Parent directory says it's synced, so our failure to retrieve the path was valid
                return None
            entry = self.source.getEntry(path)
            if entry is None:   # We checked with the source, but this path really doesn't exist
                return None
            self._registerPath(path, entry)
            pathInfo = self.metadata.getPath(dirname, basename)

        return pathInfo

    def getStatForPath(self, path):
        pathInfo = self._getPath(path)
        if pathInfo is None:
            return None

        # Compose a stat structure; fake some fields because SFTP gives us limited info:
        # 1. We fake st_nlink for directories (always 2)
        # 2. We fake st_blocks for files at always the next multiple of 512
        stat = {'st_size': pathInfo['size'], 'st_mode': pathInfo['mode'], 'st_atime': pathInfo['atime'], 'st_mtime': pathInfo['mtime'], 'st_uid': self.uid, 'st_gid': self.gid}
        if pathInfo['type'] == 'directory':
            stat['st_nlink'] = 2
        elif pathInfo['type'] == 'file':
            stat['st_blocks'] = pathInfo['size'] // 512 + 1
        return stat

    def getSymlinkTarget(self, path):
        pathInfo = self._getPath(path)
        if (pathInfo is None) or (pathInfo['type'] != 'symlink') or not pathInfo['isSynced']:
            raise RuntimeError('Unknown symlink')
        return self.target.getSymlink(path)

    def readData(self, path, offset, size):
        pathInfo = self._getPath(path)
        if (pathInfo is None) or (pathInfo['type'] != 'file'):
            raise RuntimeError('Invalid path for reading')

        # Enforce size bounds
        if (offset + size) > pathInfo['size']:
            size = max(0, pathInfo['size'] - offset)
        if size == 0:
            return ''

        # If this path is synced, we immediately return the data from source
        if pathInfo['isSynced']:
            return self.target.readData(path, offset, size)

        # Unlike read(2) suggests, many applications expect us to return exactly [size] bytes of data.
        # So we need to compile this chunk using local and remote sources, whatever is available, as long
        # as we end up with enough bytes.
        data = ''
        remoteSegments = self.metadata.getRemoteSegmentsRange(pathInfo['pathId'], offset, offset + size - 1)
        segmentIdx = 0
        currentPos = 0
        while currentPos < size:
            # Determine current remote segment properties
            if segmentIdx >= len(remoteSegments):
                segmentBegin = size
                segmentEnd = size - 1
            else:
                currentSegment = remoteSegments[segmentIdx]
                segmentBegin = currentSegment['begin'] - offset
                segmentEnd = currentSegment['end'] - offset

            # Append local data if available
            if currentPos < segmentBegin:
                data += self.target.readData(path, offset + currentPos, segmentBegin - currentPos)
                currentPos = segmentBegin

            # Append remote data
            remoteReadSize = min(size - currentPos, segmentEnd - segmentBegin + 1)
            if remoteReadSize > 0:
                data += self._downloadFileData(pathInfo, offset + currentPos, remoteReadSize)
                currentPos += remoteReadSize
                segmentIdx += 1

        return data

    def _registerPath(self, path, entry):
        if stat.S_ISDIR(entry.st_mode):
            self._registerPathDirectory(path, entry)
        elif stat.S_ISREG(entry.st_mode):
            self._registerPathFile(path, entry)
        elif stat.S_ISLNK(entry.st_mode):
            self._registerPathSymlink(path, entry)
        else:
            raise RuntimeError('Unsupported path mode: %d' % entry.st_mode)

    def _registerPathDirectory(self, path, entry):
        dirname, basename = MountLoad._splitPath(os.path.normpath(path))
        self.metadata.addPath(dirname, basename, 'directory', entry.st_size, entry.st_mode, entry.st_atime, entry.st_mtime, 0)

        self.target.createDirectory(path, entry.st_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)   # Mode u+rwx

    def _registerPathFile(self, path, entry):
        dirname, basename = MountLoad._splitPath(os.path.normpath(path))
        size = entry.st_size
        isSynced = 1 if size == 0 else 0

        self.metadata.begin()
        pathId = self.metadata.addPath(dirname, basename, 'file', size, entry.st_mode, entry.st_atime, entry.st_mtime, isSynced)
        if not isSynced:
            self.metadata.addRemoteSegment(pathId, 0, size - 1)
        self.metadata.commit()

        self.target.createFile(path, entry.st_mode | stat.S_IRUSR | stat.S_IWUSR)   # Mode u+rw

    def _registerPathSymlink(self, path, entry):
        target = self.source.getLinkTarget(path)
        self.target.createSymlink(path, target)

        dirname, basename = MountLoad._splitPath(os.path.normpath(path))
        self.metadata.addPath(dirname, basename, 'symlink', entry.st_size, entry.st_mode, entry.st_atime, entry.st_mtime, 1)

    @staticmethod
    def _splitPath(path):
        dirname, basename = os.path.split(path)
        if dirname != '/':
            dirname += '/'
        return (dirname, basename)

class MountLoadFUSE(LoggingMixIn, Operations):
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
        list = ['.', '..']
        for entry in self.mountload.getEntriesInDirectory(path):
            list.append(entry['basename'])
        return list

    def readlink(self, path):
        return self.mountload.getSymlinkTarget(path)

# Main method
if __name__ == '__main__':
    if len(argv) != 4:
        print("Usage: %s <source> <target> <mountpoint>" % argv[0])
        print("Arguments:")
        print("\tsource:\t\tsftp://user@host[:port]/path/to/source/directory")
        print("\ttarget:\t\t/path/to/target/directory")
        print("\tmountpoint:\t/path/to/mount/directory")
        
        exit(1)

    source = argv[1]
    target = argv[2]
    mountpoint = argv[3]

    ml = MountLoad(source, target)
    mlf = MountLoadFUSE(ml)
    fuse = FUSE(mlf, mountpoint, foreground=True, nothreads=True)
    exit(0)
