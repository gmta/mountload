# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

from argparse import ArgumentParser
from controller import ControllerPool
from fuseconnector import FUSEConnector
from getpass import getpass

class MountLoad:
    @staticmethod
    def run():
        """Handles command line parameters, configuration and sets up the mountload components"""

        # Parse the command line arguments
        parser = ArgumentParser(description='Mountload mounts a remote directory using SFTP while also downloading it to another target directory.')
        parser.add_argument('--debug', action='store_true', help="Enable debug mode")
        parser.add_argument('--multithreaded', action='store_true', help="Run FUSE in multithreaded mode")
        parser.add_argument('--password', action='store_true', help="Ask for an SSH password")
        parser.add_argument('source', help="The SFTP source URI, eg: sftp://user@example.org/path/to/remote/dir", nargs='?')
        parser.add_argument('target', help="The directory in which all the files should be stored")
        parser.add_argument('mountpoint', help="Path to the mountpoint")
        args = parser.parse_args()

        # Determine configuration
        source = args.source
        target = args.target
        mountpoint = args.mountpoint

        # Determine password
        password = None
        if args.password:
            password = getpass('Enter SSH password: ')

        # Initialize a controller pool and acquire a controller to check for any errors
        controllerPool = ControllerPool(source, target, password)
        try:
            controller = controllerPool.acquireController()
        except RuntimeError as e:
            parser.error('controller error: %s' % str(e))
        controllerPool.releaseController(controller)

        # Start FUSE; this will keep mountload running until unmount
        connector = FUSEConnector(controllerPool, args.debug)
        connector.startFUSE(mountpoint, isDaemonized=False, isMultiThreaded=args.multithreaded)

