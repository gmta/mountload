import os

from os.path import abspath, isdir, isfile

class MountLoadTarget:
    def __init__(self, targetDirectory):
        self.targetDirectory = abspath(targetDirectory)
        self.metaDirectory = self.targetDirectory + '/.mountload'

        # Create dirs if they don't already exist
        if not isdir(self.targetDirectory):
            os.mkdir(self.targetDirectory)
        if not isdir(self.metaDirectory):
            os.mkdir(self.metaDirectory)

    def createDirectory(self, path, mode):
        dirpath = self._normalizePath(path)
        if isdir(dirpath):
            os.chmod(dirpath, mode)
        else:
            os.mkdir(dirpath, mode)

    def createFile(self, path, mode):
        path = self._normalizePath(path)
        f = open(path, 'w')
        f.close()
        os.chmod(path, mode)

    def createSymlink(self, path, target):
        os.symlink(target, self._normalizePath(path))

    def getDBPath(self):
        return self.metaDirectory + '/metadata.sqlite'

    def getSymlink(self, path):
        return os.readlink(self._normalizePath(path))

    def _normalizePath(self, path):
        path = self.targetDirectory + path
        # TODO: redirect for self.metaDirectory
        return path

    def readData(self, path, offset, size):
        f = open(self._normalizePath(path), 'rb')
        f.seek(offset, os.SEEK_SET)
        data = f.read(size)
        f.close()
        return data

    def writeData(self, path, offset, data):
        f = open(self._normalizePath(path), 'r+b')
        f.seek(offset, os.SEEK_SET)
        f.write(data)
        f.close()
