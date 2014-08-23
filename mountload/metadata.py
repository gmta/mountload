# Copyright (c) 2014 Jelle Raaijmakers <jelle@gmta.nl>
# See the file LICENSE.txt for copying permission.

import sqlite3

class MountLoadMetaData:
    metaDataVersion = 1

    def __init__(self, dbpath):
        self.conn = sqlite3.connect(database=dbpath, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.transactionDepth = 0

        # Check whether the config table exists
        c = self.conn.execute('SELECT 1 FROM sqlite_master WHERE type = \'table\' AND name = \'config\'')
        if not c.fetchone():
            self._createEmptyDB()
            return

        # Check whether we need to upgrade the metadata database
        version = self.getConfigInteger('version')
        if version is None:
            raise RuntimeError('Corrupted metadata configuration')
        elif version < MountLoadMetaData.metaDataVersion:
            self._upgradeDB(version)

    def addPath(self, dirname, basename, pathType, size, mode, atime, mtime, isSynced):
        c = self.conn.cursor()
        c.execute('INSERT INTO path (dirname, basename, type, size, mode, atime, mtime, isSynced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (dirname, basename, pathType, size, mode, atime, mtime, isSynced))
        return c.lastrowid

    def addRemoteSegment(self, pathId, begin, end):
        self.conn.execute('INSERT INTO remoteSegment (path, begin, end) VALUES (?, ?, ?)', (pathId, begin, end))

    def begin(self):
        if self.transactionDepth == 0:
            self.conn.execute('BEGIN IMMEDIATE')
        self.transactionDepth += 1

    def close(self):
        if self.transactionDepth > 0:
            self.rollback()
        self.conn.close()

    def commit(self):
        if self.transactionDepth == 0:
            raise RuntimeError('No transaction started')
        self.transactionDepth -= 1
        if self.transactionDepth == 0:
            self.conn.execute('COMMIT')

    def _createEmptyDB(self):
        # Create tables
        c = self.conn.cursor()
        c.execute('CREATE TABLE config (name TEXT PRIMARY KEY, value TEXT)')
        c.execute('CREATE TABLE path (pathId INTEGER PRIMARY KEY, dirname TEXT, basename TEXT, type TEXT, size INTEGER, mode INTEGER, atime INTEGER, mtime INTEGER, isSynced INTEGER, UNIQUE (dirname, basename))')
        c.execute('CREATE TABLE remoteSegment (remoteSegmentId INTEGER PRIMARY KEY, path INTEGER, begin INTEGER, end INTEGER, FOREIGN KEY (path) REFERENCES path (pathId))')
        c.execute('CREATE INDEX remoteSegment_path_idx ON remoteSegment (path)')

        # Register current scheme version
        self.setConfig('version', MountLoadMetaData.metaDataVersion)

    def getConfigInteger(self, name):
        v = self.getConfigString(name)
        return None if v is None else int(v)

    def getConfigString(self, name):
        c = self.conn.cursor()
        c.execute('SELECT value FROM config WHERE name = ?', (name,))
        r = c.fetchone()
        return None if r is None else r[0]

    def getPath(self, dirname, basename):
        return self.conn.execute('SELECT * FROM path WHERE dirname = ? AND basename = ?', (dirname, basename)).fetchone()

    def getRemoteSegments(self, pathId):
        return self.conn.execute('SELECT * FROM remoteSegment WHERE path = ?', (pathId,)).fetchall()

    def getRemoteSegmentsRange(self, pathId, begin, end):
        sql = '''
            SELECT *
            FROM remoteSegment
            WHERE path = ?
                AND begin <= ?
                AND end >= ?
            ORDER BY begin ASC
        '''
        return self.conn.execute(sql, (pathId, end, begin)).fetchall()

    def getSubPaths(self, directoryPath):
        return self.conn.execute('SELECT * FROM path WHERE dirname = ? AND basename <> \'\'', (directoryPath,)).fetchall()

    def removeRemoteSegments(self, pathId, begin, end):
        self.begin()
        c = self.conn.cursor()
        for segment in self.getRemoteSegmentsRange(pathId, begin, end):
            segId = segment['remoteSegmentId']
            if (segment['begin'] >= begin) and (segment['end'] <= end):
                # Segment is contained by region to delete
                c.execute('DELETE FROM remoteSegment WHERE remoteSegmentId = ?', (segId,))
            elif (begin > segment['begin']) and (end < segment['end']):
                # Segment contains region to delete, so split it
                c.execute('UPDATE remoteSegment SET end = ? WHERE remoteSegmentId = ?', (begin - 1, segId))
                self.addRemoteSegment(pathId, end + 1, segment['end'])
            elif (begin > segment['begin']) and (begin <= segment['end']):
                # Segment ends with region to delete, so shorten it
                c.execute('UPDATE remoteSegment SET end = ? WHERE remoteSegmentId = ?', (begin - 1, segId))
            elif (end >= segment['begin']) and (end < segment['end']):
                # Segment begins with region to delete, so shorten it
                c.execute('UPDATE remoteSegment SET begin = ? WHERE remoteSegmentId = ?', (end + 1, segId))
            else:
                # Should never happen
                raise RuntimeError('Invalid remote segment matched')
        self.commit()

    def rollback(self):
        if self.transactionDepth == 0:
            raise RuntimeError('No active transaction')
        self.conn.execute('ROLLBACK')
        self.transactionDepth = 0

    def setConfig(self, name, value):
        self.conn.execute('INSERT INTO config (name, value) VALUES (?, ?)', (name, value))

    def setPathSynced(self, pathId):
        self.conn.execute('UPDATE path SET isSynced = 1 WHERE pathId = ?', (pathId,))

    def _upgradeDB(self, fromVersion):
        raise NotImplementedError('No upgrade paths available yet')

