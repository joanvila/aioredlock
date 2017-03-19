aioredlock
==========

.. image:: https://travis-ci.org/joanvila/aioredlock.svg?branch=master
  :target: https://travis-ci.org/joanvila/aioredlock

.. image:: https://codecov.io/gh/joanvila/aioredlock/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/joanvila/aioredlock

.. image:: https://badge.fury.io/py/aioredlock.svg
  :target: https://pypi.python.org/pypi/aioredlock

The asyncio redlock_ algorithm implementation.

Redlock and asyncio
-------------------

The redlock algorithm is a distributed lock implementation for Redis_. There are many implementations of it in several languages. In this case, this is the asyncio_ compatible implementation for python 3.5+.


Usage
-----
.. code-block:: python

  from aioredlock import Aioredlock

  # Define a list of connections to your Redis instances:
  redis_instances = [
    {'host': 'localhost', 'port': 6379}
  ]

  # Create a lock manager:
  lock_manager = Aioredlock(redis_instances)

  # Try to acquire the lock:
  lock = await lock_manager.lock("resource_name")

  # Release the lock:
  await lock_manager.unlock(lock)

  # Clear the connections with Redis
  await lock_manager.destroy()


How it works
------------

The Aioredlock constructor takes a list of connections (host and port) where the Redis instances are running as a required parameter.
In order to acquire the lock, the ``lock`` function should be called. If the lock operation is successful, ``lock.valid`` will be true.

From that moment, the lock is valid until the ``unlock`` function is called or when the 10 seconds timeout is reached.

In order to clear all the connections with Redis, the lock_manager ``destroy`` method can be called.

To-do
-----

* Allow the user to set a desired lock timeout with 10 seconds default
* Raise an exception if the lock cannot be obtained so no need to check for `lock.valid`
* Handle/encapsulate aioredis exceptions when performing operations
* Expire the lock valid attribute according to the lock validity in a safe way if possible
* Lock extension

.. _redlock: https://redis.io/topics/distlock
.. _Redis: https://redis.io
.. _asyncio: https://docs.python.org/3/library/asyncio.html
