aioredlock
==========

.. image:: https://travis-ci.org/joanvila/aioredlock.svg?branch=master
  :target: https://travis-ci.org/joanvila/aioredlock

The asyncio redlock_ algorithm implementation.

Redlock and asyncio
-------------------

The redlock algorithm is a distributed lock implementation for Redis_. There are many implementations of it in several languages. In this case, this is the asyncio_ compatible implementation for python.


Usage
-----

Create a lock manager instance:

``lock_manager = Aioredlock()``

Try to get the acquire the lock:

``lock = await lock_manager.lock("resource_name", 10)``

Release the lock:

``await lock_manager.unlock(lock)``

To-do
-----

* Support multiple redis instances
* Add drift time
* Lock extension

.. _redlock: https://redis.io/topics/distlock
.. _Redis: https://redis.io
.. _asyncio: https://docs.python.org/3/library/asyncio.html
