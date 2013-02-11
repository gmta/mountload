# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

from argparse import ArgumentParser
from controller import Controller
from fuseconnector import FUSEConnector

class MountLoad:
    @staticmethod
    def run():
        """Handles command line parameters, configuration and sets up the mountload components"""

        # Parse the command line arguments
        parser = ArgumentParser(description='Mountload mounts a remote directory using SFTP while also downloading it to another target directory.')
        parser.add_argument('--password', action='store_true', help="Ask for an SSH password")
        parser.add_argument('source', help="The SFTP source URI, eg: sftp://user@example.org/path/to/remote/dir", nargs='?')
        parser.add_argument('target', help="The directory in which all the files should be stored")
        parser.add_argument('mountpoint', help="Path to the mountpoint")
        args = parser.parse_args()

        # Determine options
        askPassword = args.password
        source = args.source
        target = args.target
        mountpoint = args.mountpoint

        # Run mountload
        controller = Controller(source, target, askPassword)
        connector = FUSEConnector(controller)
        connector.startFUSE(mountpoint, isDaemonized=False, isMultiThreaded=False)
