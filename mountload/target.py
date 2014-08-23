# Copyright (c) 2014 Jelle Raaijmakers <jelle@gmta.nl>
# See the file LICENSE.txt for copying permission.

import os
from os.path import abspath, isdir

class MountLoadTarget:
    def __init__(self, targetDirectory):
        self.databaseFilename = 'metadata.sqlite'
        self.targetDirectory = abspath(targetDirectory)
        self.metaDirectory = self.targetDirectory + '/.mountload'
        self.redirectionDirectory = self.metaDirectory + '/redirect'

        self._ensureDirectoriesExist([self.targetDirectory, self.metaDirectory, self.redirectionDirectory])

    def close(self):
        pass

    def createDirectory(self, relativePath, mode):
        dirpath = self._normalizePath(relativePath)
        if isdir(dirpath):
            os.chmod(dirpath, mode)
        else:
            os.mkdir(dirpath, mode)

    def createFile(self, relativePath, mode):
        path = self._normalizePath(relativePath)
        f = open(path, 'w')
        f.close()
        os.chmod(path, mode)

    def createSymlink(self, relativePath, target):
        os.symlink(target, self._normalizePath(relativePath))

    def _ensureDirectoriesExist(self, directories):
        for directory in directories:
            if not isdir(directory):
                os.mkdir(directory)

    def getDBPath(self):
        return self.metaDirectory + '/' + self.databaseFilename

    def getSymlink(self, relativePath):
        return os.readlink(self._normalizePath(relativePath))

    def _normalizePath(self, relativePath):
        """
        Constructs the absolute path for a path relative to the root of our
        mountpoint. If this path would happen to conflict with our metadata
        directory, we redirect the entire path to our redirections directory.
        """
        absolutePath = self.targetDirectory + relativePath
        if absolutePath == self.metaDirectory or absolutePath.startswith(self.metaDirectory + '/'):
            absolutePath = self.redirectionDirectory + relativePath
        return absolutePath

    def readData(self, relativePath, offset, size):
        f = open(self._normalizePath(relativePath), 'rb')
        f.seek(offset, os.SEEK_SET)
        data = f.read(size)
        f.close()
        return data

    def writeData(self, relativePath, offset, data):
        f = open(self._normalizePath(relativePath), 'r+b')
        f.seek(offset, os.SEEK_SET)
        f.write(data)
        f.close()
