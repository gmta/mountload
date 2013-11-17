mountload
=========

Mountload is a FUSE library written in Python that allows you to mount a remote directory over SFTP, while simultaneously downloading the directory to a second target location.

You can access the files in the mount directory as you normally would, but as soon as parts of the files have been downloaded mountload will use the target location as a local cache. You should use mountload if you want to immediately access remote files and directories without waiting for them to download first.

requirements
============

- fuse 2.9.0+
- python 2.7.3+
- python-paramiko 1.7.7+

examples
========
To mount a remote directory:

    ./mountload.py sftp://user@example.org/path/to/remote/directory /path/to/copytarget /path/to/mount

After mounting the source URI once, you only need to supply the target and mountpoint. The source URI is stored in the metadata database in the target:

    ./mountload.py /path/to/copytarget /path/to/mount

notes
=====

At the moment, mountload is in a severe alpha state and as such knows many limitations and quirks:

- Downloading data will only happen on read() requests; background downloading is planned
- SFTP network throughput has not been optimized as much as it could be
- It will not notify you when all files have been downloaded
- The source is expected to be read-only
- It will probably burn down your house and steal your car

license
=======

Mountload is released under MIT license. See license.txt for details.
