from errno import ENOENT
from os.path import normpath
from paramiko import SSHClient
from urlparse import urlsplit

class MountLoadSource:
    def __init__(self, sourceURI):
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

        # Connect
        self.client = SSHClient()
        self.client.load_system_host_keys()
        self.client.connect(hostname=hostname, username=username, port=port, compress=True)

        self.sftp = self.client.open_sftp()
        self.sftp.chdir(self.remoteDirectory)

    def close(self):
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

    def readData(self, path, offset, size):
        f = self.sftp.open(self.remoteDirectory + path, 'r')
        f.seek(offset, 0)
        data = f.read(size)
        f.close()
        return data
