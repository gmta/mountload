# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

from errno import ENOENT
from os.path import normpath
from paramiko import SSHClient, WarningPolicy
from urllib.parse import urlsplit

class MountLoadSource:
    def __init__(self, sourceURI, password):
        # Split the source URI into components
        components = urlsplit(sourceURI)
        hostname = components.hostname
        username = 'anonymous' if components.username is None else components.username
        port = 22 if components.port is None else components.port
        remoteDirectory = components.path

        # Normalize path
        remoteDirectory = normpath(remoteDirectory)
        if not remoteDirectory.startswith('/'):
            raise RuntimeError('Remote directory %s is not an absolute path' % remoteDirectory)
        self.remoteDirectory = remoteDirectory

        # Connect using SSH
        self.client = SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(WarningPolicy())
        self.client.connect(hostname=hostname, port=port, username=username, password=password, compress=True)

        # Open SFTP channel over SSH
        self.sftp = self.client.open_sftp()

        # Keep track of the last opened file
        self.lofFP = None
        self.lofPath = None

    def close(self):
        # Do a del so SFTPFile performs an async close; this is necessary because we can't guarantee it's being closed
        # by the same thread that opened the file. This happens during FUSE destroy() for example.
        if self.lofFP is not None:
            del self.lofFP

        # Close both SFTPClient and SSHClient; these work fine across threads
        self.sftp.close()
        self.client.close()

    def getDirectoryEntries(self, path):
        return self.sftp.listdir_attr(self.remoteDirectory + path)

    def getEntry(self, path):
        try:
            stat = self.sftp.stat(self.remoteDirectory + path)
        except IOError as e:
            if e.errno == ENOENT:
                return None
            raise
        return stat

    def getLinkTarget(self, path):
        return self.sftp.readlink(self.remoteDirectory + path)

    def getRemoteDirectory(self):
        return self.remoteDirectory

    def readData(self, path, offset, size):
        # Open the file if not already open
        if self.lofPath != path:
            if self.lofFP is not None:
                self.lofFP.close()
            self.lofFP = self.sftp.open(self.remoteDirectory + path, 'r')
            self.lofPath = path

        # Perform prefetched reads
        datachunks = self.lofFP.readv([(offset, size)])
        return b''.join(datachunks)
