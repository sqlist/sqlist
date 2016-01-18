# -*- coding: utf-8 -*-

import sqlite3
import pickle


class SQLiteSequence(object):
    __statements = [
        'CREATE TABLE IF NOT EXISTS %s(key BLOB, value BLOB NOT NULL);',
        'CREATE INDEX IF NOT EXISTS key_idx ON %s(key);'
    ]

    def __init__(self, filename, autocommit=True, drop=False, journal_mode='DELETE', key=None, table_name='data',
                 serializer=pickle):
        if not (hasattr(serializer, 'dumps') or hasattr(serializer, 'loads')):
            raise TypeError('serializer must implement loads and dumps methods')
        self.serializer = serializer
        self.autocommit = autocommit
        self.table_name = table_name
        journal_modes = ['WAL', 'OFF', 'DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY']
        if journal_mode not in journal_modes:
            raise ValueError('journal_mode must be one of %s' % journal_modes)
        self.db = sqlite3.connect(filename)
        self.cursor = self.db.cursor()
        if drop:
            self.cursor.execute('DROP TABLE IF EXISTS %s;' % self.table_name)
        self.cursor.execute('PRAGMA journal_mode = %s' % journal_mode)
        for statement in self.__statements:
            self.cursor.execute(statement % self.table_name)
        self.commit()

        if key:
            if callable(key):
                self.key = key
            else:
                raise TypeError('%s object is not callable' % type(key))
        else:
            self.key = lambda _: None

    def __len__(self):
        return self.cursor.execute('SELECT COUNT(*) FROM %s;' % self.table_name).fetchone()[0]

    def __contains__(self, item):
        return bool(
            self.cursor.execute('SELECT oid FROM %s WHERE value = ?;' % self.table_name,
                                [self.pack(item)]).fetchone()
        )

    def __bool__(self):
        return bool(self.cursor.execute('SELECT oid FROM %s LIMIT 1;' % self.table_name).fetchone())

    def __iter__(self):
        for item in self.cursor.execute('SELECT value FROM %s;' % self.table_name):
            yield self.unpack(item[0])
        raise StopIteration

    def commit(self):
        self.db.commit()

    def flush(self):
        self.cursor.execute('DELETE FROM %s;' % self.table_name)
        self.commit()

    def pack(self, value):
        return sqlite3.Binary(self.serializer.dumps(value))

    def unpack(self, value):
        return self.serializer.loads(value)

    def append(self, value):
        raise NotImplementedError

    def extend(self, lst):
        raise NotImplementedError

    def pop(self, index=-1):
        raise NotImplementedError

    def __getitem__(self, item):
        raise NotImplementedError

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def __eq__(self, other):
        raise NotImplementedError


class SQList(object):
    """
    List-like object that stores data in SQLite database
    """
    def __init__(self, values=None, path=':memory:', key=None, drop=True):
        """
        :param values: iterable with elements of new list
        :param path: path to file with SQLite database
        :param key: callable object used to sort values of SQList
        :param drop: drop any data in file at path
        """
        if key:
            if callable(key):
                self.key = key
            else:
                raise TypeError('%s object is not callable' % type(key))
        else:
            self.key = lambda x: None

        self.path = path
        self.sql = sqlite3.connect(path)
        self.sql.text_factory = str
        self.cursor = self.sql.cursor()
        if drop:
            self.cursor.execute('''DROP TABLE IF EXISTS `data`;''')
        self.cursor.execute(
            '''CREATE TABLE `data` (`key` BLOB,
                                    `value` BLOB NOT NULL);''')
        self.cursor.execute('''CREATE INDEX `keys_index` ON `data` (`key`);''')
        if values:
            self.cursor.executemany(
                '''INSERT INTO `data`
                   (`key`, `value`)
                   VALUES (?, ?);''',
                zip(map(self.key, values), map(pickle.dumps, values))
            )
            self.sql.commit()

    def __repr__(self):
        return 'sqlist.SQList([%s])' % ', '.join(map(repr, self[:50]))

    def __len__(self):
        result = self.cursor.execute(
            '''SELECT COUNT(*) FROM `data`;'''
        ).fetchone()
        return result[0]

    def __getitem__(self, index):
        if type(index) == int:
            offset, stop, stride = slice(index, index + 1).indices(len(self))
        elif type(index) == slice:
            offset, stop, stride = index.indices(len(self))
        else:
            raise TypeError('Int or slice expected, %s found' % type(index))
        limit = stop - offset

        result = self.cursor.execute(
                '''SELECT `value`
                   FROM `data`
                   ORDER BY `key` ASC
                   LIMIT ? OFFSET ?;''',
                (limit, offset)
        )

        if type(index) == int:
            result = result.fetchone()
            if result is None:
                raise IndexError('%s is out of range' % index)
            else:
                return pickle.loads(result[0])
        else:
            return [pickle.loads(i[0]) for i in result]

    def __setitem__(self, index, value):
        offset, stop, stride = slice(index, index + 1).indices(len(self))

        result = self.cursor.execute(
            '''UPDATE `data`
               SET `key` = ?, `value` = ?
               WHERE `_rowid_` = (
                  SELECT `_rowid_`
                  FROM `data`
                  ORDER BY `key` ASC
                  LIMIT 1 OFFSET ?
               );''',
            (self.key(value), pickle.dumps(value), offset)
        )
        if not result.rowcount:
            raise IndexError('%s is out of range' % index)
        else:
            self.sql.commit()

    def __delitem__(self, index):
        offset, stop, stride = slice(index, index + 1).indices(len(self))

        result = self.cursor.execute(
            '''DELETE FROM `data`
               WHERE `_rowid_` = (
                  SELECT `_rowid_`
                  FROM `data`
                  ORDER BY `key` ASC
                  LIMIT 1 OFFSET ?
               );''',
            (offset, )
        )
        if not result.rowcount:
            raise IndexError('%s is out of range' % index)
        else:
            self.sql.commit()

    def __iter__(self):
        for item in self.cursor.execute(
                '''SELECT `value` FROM `data` ORDER BY `key` ASC;'''):
            yield pickle.loads(item[0])
        raise StopIteration

    def __contains__(self, item):
        result = self.cursor.execute(
            '''SELECT `_rowid_`
               FROM `data`
               WHERE `value` = ?;''',
            (pickle.dumps(item),)
        )
        return bool(result.fetchone())

    def __eq__(self, other):
        if not hasattr(other, '__len__') and len(self) != len(other):
                return False

        this = self.cursor.execute(
                '''SELECT `value` FROM `data` ORDER BY `key` ASC;''')
        for a, b in zip((pickle.loads(i[0]) for i in this), other):
            if a != b:
                return False
        return True

    def append(self, value):
        result = self.cursor.execute(
            '''INSERT INTO `data`
               (`key`, `value`)
               VALUES (?, ?);''',
            (self.key(value), pickle.dumps(value))
        )
        self.sql.commit()

    def pop(self, index=-1):
        offset, stop, stride = slice(index, index + 1).indices(len(self))

        self.cursor.execute('BEGIN TRANSACTION;')
        result = self.cursor.execute(
            '''SELECT `_rowid_`, `value`
               FROM `data`
               ORDER BY `key` ASC
               LIMIT 1 OFFSET ?;''',
            (offset, )
        )
        if result.rowcount:
            rowid, value = result.fetchone()
            self.cursor.execute(
                '''DELETE FROM `data` WHERE `_rowid_` = ?;''', (rowid, )
            )
            self.sql.commit()
            return pickle.loads(value)
        else:
            raise IndexError('{} is out of range'.format(index))

    def sort(self, key=None, reverse=False):
        def swap_required(a, b):
            """Check if items should be swapped considering reverse order"""
            try:
                if reverse:
                    return a < b
                else:
                    return a > b
            except TypeError as e:
                # Values are not comparable
                self.sql.rollback()
                raise TypeError(e)

        if key:
            if not callable(key):
                raise TypeError('{} object is not callable'.format(type(key)))
        else:
            def key(x):
                return x

        if self.key:
            self.cursor.execute('''UPDATE `data` SET `key` = NULL;''')
            self.key = None
        position = 0
        while True:
            values = self.cursor.execute(
                '''SELECT `_rowid_`, `value` FROM `data` LIMIT 2 OFFSET ?''',
                (position, )
            ).fetchall()
            if len(values) < 2:
                break
            unpacked = [pickle.loads(i[1]) for i in values]
            q = ((values[1][1], values[1][0]), (values[0][1], values[0][0]))
            if swap_required(key(unpacked[0]), key(unpacked[1])):
                self.cursor.executemany(
                        '''UPDATE `data` SET `value` = ? WHERE `_rowid_` = ?''',
                        ((values[1][1], values[0][0]),
                         (values[0][1], values[1][0]))
                )
                if position:
                    position -= 1
            else:
                position += 1


def open(filename, key=None, drop=True):
    return SQList(path=filename, key=key, drop=drop)
