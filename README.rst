aioredlock
==========

.. image:: https://travis-ci.org/joanvila/aioredlock.svg?branch=master
  :target: https://travis-ci.org/joanvila/aioredlock

.. image:: https://codecov.io/gh/joanvila/aioredlock/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/joanvila/aioredlock

The asyncio redlock_ algorithm implementation.

Redlock and asyncio
-------------------

The redlock algorithm is a distributed lock implementation for Redis_. There are many implementations of it in several languages. In this case, this is the asyncio_ compatible implementation for python.


Usage
-----
.. code-block:: python

  from aioredlock import Aioredlock

  # Create a lock manager instance:
  lock_manager = Aioredlock(host='localhost', port=6379)

  # Try to acquire the lock:
  lock = await lock_manager.lock("resource_name")

  # Release the lock:
  await lock_manager.unlock(lock)

  # Clear the connections with Redis
  await lock_manager.destroy()


How it works
------------

The Aioredlock constructor takes the host and the port where the Redis instance is running as parameters.
In order to acquire the lock, the ``lock`` function should be called. If the lock operation is successful, ``lock.valid`` will be true.

From that moment, the lock is valid until the ``unlock`` function is called or when the 10 seconds timeout is reached.

In order to clear all the connections with Redis, the lock_manager ``destroy`` method can be called.

To-do
-----

* Support multiple redis instances
* Add drift time
* Lock extension

.. _redlock: https://redis.io/topics/distlock
.. _Redis: https://redis.io
.. _asyncio: https://docs.python.org/3/library/asyncio.html
