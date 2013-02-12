# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

from metadata import MountLoadMetaData
from os import getgid, getuid
import os.path
from source import MountLoadSource
import stat
from target import MountLoadTarget
from threading import Condition

class Controller:
    def __init__(self, sourceURI, targetDirectory, password):
        self.gid = getgid()
        self.uid = getuid()

        # Initialize target and metadata
        self.target = MountLoadTarget(targetDirectory)
        self.metadata = MountLoadMetaData(self.target.getDBPath())

        # Store and check source URI
        knownSourceURI = self.metadata.getConfigString('sourceURI')
        if sourceURI is None:
            sourceURI = knownSourceURI
        elif knownSourceURI is None:
            self.metadata.setConfig('sourceURI', sourceURI)
        elif knownSourceURI != sourceURI:
            raise RuntimeError('Given source URI differs from known source URI')

        # Initialize source
        self.source = MountLoadSource(sourceURI, password)

        # Bootstrap the remote root
        self.metadata.begin()
        if self._getPath('/') is None:
            self._registerPath('/', self.source.getEntry('/'))
        self.metadata.commit()

    def close(self):
        self.source.close()
        self.metadata.close()
        self.target.close()

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
        dirname, basename = Controller._splitPath(path)
        pathInfo = self.metadata.getPath(dirname, basename)

        # If no path was found, recursively check parent directory for sync
        if (pathInfo is None) and (path != '/'):
            parentDirInfo = self._getPath(os.path.dirname(path))
            if parentDirInfo is None:  # Parent directory doesn't exist, so this path can't exist either
                return None
            if parentDirInfo['isSynced']:  # Parent directory says it's synced, so our failure to retrieve the path was valid
                return None
            entry = self.source.getEntry(path)
            if entry is None:  # We checked with the source, but this path really doesn't exist
                return None
            self._registerPath(path, entry)
            pathInfo = self.metadata.getPath(dirname, basename)

        return pathInfo

    def getStatForPath(self, path):
        pathInfo = self._getPath(path)
        if pathInfo is None:
            return None

        # Compose a stat structure; fake some fields because SFTP gives us limited info:
        # 1. We fake st_blocks, assuming FS block size of 4 KiB and stat block size of 512 bytes:
        #    * Calculate number of 4 KiB blocks, ceil() using integer division
        #    * Multiply by 8 (4 KiB / 512 bytes) to obtain the number of blocks
        # 2. We fake st_nlink for directories (2) and files (1)
        stat = {'st_size': pathInfo['size'], 'st_mode': pathInfo['mode'], 'st_atime': pathInfo['atime'],
                'st_mtime': pathInfo['mtime'], 'st_uid': self.uid, 'st_gid': self.gid}
        stat['st_blocks'] = (pathInfo['size'] + 4095) // 4096 * 8
        if pathInfo['type'] == 'directory':
            stat['st_nlink'] = 2
        elif pathInfo['type'] == 'file':
            stat['st_nlink'] = 1
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
            raise RuntimeError('Unsupported path mode %d for path %s' % (entry.st_mode, path))

    def _registerPathDirectory(self, path, entry):
        dirname, basename = Controller._splitPath(os.path.normpath(path))
        self.metadata.addPath(dirname, basename, 'directory', entry.st_size, entry.st_mode, entry.st_atime, entry.st_mtime, 0)

        self.target.createDirectory(path, entry.st_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)  # Mode u+rwx

    def _registerPathFile(self, path, entry):
        dirname, basename = Controller._splitPath(os.path.normpath(path))
        size = entry.st_size
        isSynced = 1 if size == 0 else 0

        self.metadata.begin()
        pathId = self.metadata.addPath(dirname, basename, 'file', size, entry.st_mode, entry.st_atime, entry.st_mtime, isSynced)
        if not isSynced:
            self.metadata.addRemoteSegment(pathId, 0, size - 1)
        self.metadata.commit()

        self.target.createFile(path, entry.st_mode | stat.S_IRUSR | stat.S_IWUSR)  # Mode u+rw

    def _registerPathSymlink(self, path, entry):
        target = self.source.getLinkTarget(path)
        self.target.createSymlink(path, target)

        dirname, basename = Controller._splitPath(os.path.normpath(path))
        self.metadata.addPath(dirname, basename, 'symlink', entry.st_size, entry.st_mode, entry.st_atime, entry.st_mtime, 1)

    @staticmethod
    def _splitPath(path):
        dirname, basename = os.path.split(path)
        if dirname != '/':
            dirname += '/'
        return (dirname, basename)

class ControllerPool:
    """ControllerPool is a Controller factory which maintains a pool of Controller instances"""
    maximumNumberOfInstances = 4

    def __init__(self, sourceURI, targetDirectory, password):
        self.instanceArguments = {'sourceURI': sourceURI, 'targetDirectory': targetDirectory, 'password': password}

        # Instance pool
        self.availableInstances = []
        self.numberOfInstances = 0
        self.poolCondition = Condition()

    def acquireController(self):
        """Acquires a Controller instance instantly or waits while one becomes available"""
        self.poolCondition.acquire()
        while not self._isInstanceAvailable():
            self.poolCondition.wait()
        controller = self._acquireInstance()
        self.poolCondition.release()
        return controller

    def _acquireInstance(self):
        if len(self.availableInstances) == 0:
            newInstance = Controller(**self.instanceArguments)
            self.numberOfInstances += 1
            return newInstance
        return self.availableInstances.pop()

    def close(self):
        self.poolCondition.acquire()
        while len(self.availableInstances) < self.numberOfInstances:
            self.poolCondition.wait()
        for instance in self.availableInstances:
            instance.close()
        del self.availableInstances
        del self.numberOfInstances
        self.poolCondition.release()

    def _isInstanceAvailable(self):
        return len(self.availableInstances) > 0 or self.numberOfInstances < ControllerPool.maximumNumberOfInstances

    def releaseController(self, controller):
        """Returns a Controller instance to the available pool"""
        self.poolCondition.acquire()
        self.availableInstances.append(controller)
        self.poolCondition.notify()
        self.poolCondition.release()
