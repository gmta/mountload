mountload
=========

Mountload is a userspace FUSE library for Linux that allows you to mount a remote directory over SFTP, while simultaneously downloading the directory to a second target location. Mountload is written in Python.

You can access the files in the mount directory as you normally would, but as soon as parts of the files have been downloaded mountload will use the target location as a local cache. You should use mountload if you want to immediately access remote files and directories without waiting for them to download first.

requirements
============

- fuse 2.9.0+
- python 2.7.3+
- python-paramiko 1.7.7+

example
=======

    ./mountload sftp://user@example.org/path/to/remote/directory /path/to/copytarget /path/to/mount

notes
=====

At the moment, mountload is in a severe alpha state and as such knows many limitations and quirks:

- Downloading data will only happen on read() requests; background downloading is planned
- SFTP access will only succeed if you use SSH authentication keys (id\_rsa and authorized\_keys)
- SFTP network throughput has not been optimized; expect horrible speeds
- It will not tell you if all files have been downloaded
- The source is expected to be read-only
- Access through FUSE is single threaded
- It will probably burn down your house and steal your car
