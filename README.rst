.. image:: https://travis-ci.org/sqlist/sqlist.svg?branch=0.2
    :target: https://travis-ci.org/sqlist/sqlist

SQList is like `shelve <https://docs.python.org/3/library/shelve.html>`_ but for lists. It serializes data with `pickle <https://docs.python.org/3/library/pickle.html>`_ and puts into SQLite database.

Usage
=====

SQList object behaves mostly like casual Python list. For example you can create SQList from list.

>>> import sqlist
>>> values = sqlist.SQList([1, 2, 3])
>>> values
sqlist.SQList([1, 2, 3])

You can get item from list by index.

>>> values[0]
1
>>> values[-1]
3

Slices are available too.

>>> values[0:2]
[1, 2]

You can easily append item to SQList and pop them from it.

>>> values.append(23)
>>> values
sqlist.SQList([1, 2, 3, 23])
>>> valuea.pop(0)
1
>>> values
sqlist.SQList([2, 3, 23])

Unfortunately inserting is not available yet but planned for future versions.


Sorting
=======

You can easily sort SQList just in place, but be careful, this sorting can be quite long.

>>> values.sort(key=lambda x: 1 / x)
>>> values
sqlist.SQList([23, 3, 2])
>>> values.sort()
>>> values
sqlist.SQList([2, 3, 23])

Also there is a way to keep your SQList permanently sorted.

>>> values = sqlist.SQList(['a', 'aa', 'aaaa'], key=lambda x: len('x'))
>>> values
sqlist.SQList(['a', 'aa', 'aaaa'], key=lambda x: len('x'))
>>> values.append('bbb')
>>> values
sqlist.SQList(['a', 'aa', 'bbb', 'aaaa'], key=lambda x: len('x'))

This way is a bit ugly but it was designed for high performance.

Multithreading
==============

At the moment SQList is not thread safe, may be it will become so later.
