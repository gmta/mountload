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
        parser = ArgumentParser(description='Mountload mounts a remote directory using SFTP and simultaneously downloads it to another target directory.')

        grp_ml = parser.add_argument_group('Mountload arguments')
        grp_ml.add_argument('--debug', action='store_true', help="Enable debug mode")
        grp_ml.add_argument('--password', action='store_true', help="Ask for an SSH password")
        grp_ml.add_argument('source', help="The SFTP source URI, eg: sftp://user@example.org/path/to/remote/dir", nargs='?')
        grp_ml.add_argument('target', help="The directory in which all the files should be stored")
        grp_ml.add_argument('mountpoint', help="Path to the mountpoint")

        grp_fuse = parser.add_argument_group('FUSE arguments')
        grp_fuse.add_argument('--multithreaded', action='store_true', help="Use multiple threads for filesystem access")

        args = parser.parse_args()

        # Determine configuration
        source = args.source
        target = args.target
        mountpoint = args.mountpoint

        # Determine password
        password = None
        if args.password:
            password = getpass('Enter SSH password: ')

        # Initialize a controller pool and acquire a controller to check for any initial errors
        controllerPool = ControllerPool(source, target, password)
        try:
            with controllerPool.acquire():
                pass
        except RuntimeError as e:
            parser.error('controller error: %s' % str(e))

        # Start FUSE; this will keep mountload running until unmount
        connector = FUSEConnector(controllerPool, args.debug)
        connector.startFUSE(mountpoint, isDaemonized=False, isMultiThreaded=args.multithreaded)

