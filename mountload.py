#!/usr/bin/env python
# Copyright (c) 2013 Jelle Raaijmakers <jelle@gmta.nl>
# See the file license.txt for copying permission.

# Add paths to dependencies
import sys
sys.path.append('vendor/fusepy/')
sys.path.append('vendor/paramiko/')

from mountload.mountload import MountLoad

# Run mountload
if __name__ == '__main__':
    MountLoad.run()
