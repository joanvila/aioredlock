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

  from aioredlock import Aioredlock, LockError

  # Define a list of connections to your Redis instances:
  redis_instances = [
    ('localhost', 6379),
    {'host': 'localhost', 'port': 6379, 'db': 1},
    'redis://localhost:6379/2',
  ]

  # Create a lock manager:
  lock_manager = Aioredlock(redis_instances)

  # Check is lock acquired by another redlock instance
  assert not await lock_manager.is_locked("resource_name")

  # Try to acquire the lock:
  try:
      lock = await lock_manager.lock("resource_name")
  except LockError:
      print('Lock not acquired')
      raise

  # Now lock is acquired
  assert lock.valid
  assert await lock_manager.is_locked("resource_name")

  # extend lock's lifetime
  await lock_manager.extend(lock)
  # raises LockError if can not extend lock lifetime
  # on more then half redis instances

  # Release the lock:
  await lock_manager.unlock(lock)
  # raises LockError if can not release lock
  # on more then half redis instances

  # After release lock become invalid
  assert not lock.valid
  assert not await lock_manager.is_locked("resource_name")

  # Or you can use locks with context manager
  try:
      async with await lock_manager.lock("resource_name") as lock:
          assert lock.valid is True
          # Do your stuff having the lock
          await lock.extend()  # alias for lock_manager.extend(lock)
          # Do more stuff having the lock
      assert lock.valid is False # lock will be released by context manager
  except LockError:
      print('Lock not acquired')
      raise

  # Clear the connections with Redis
  await lock_manager.destroy()


How it works
------------

The Aioredlock constructor accepts the following optional parameters:

- ``redis_connections``: A list of connections (dictionary of host and port and kwargs for `aioredis.create_redis_pool()`, or tuple `(host, port)`, or sting Redis URI) where the Redis instances are running.  The default value is ``[{'host': 'localhost', 'port': 6379}]``.
- ``lock_timeout``: An integer (in milliseconds) representing lock validity period. The default value is ``10000`` ms.
- ``drift``: An integer for clock drift compensation. The default value is calculated by ``int(lock_timeout * 0.01) + 2`` ms.
- ``retry_count``: An integer representing number of maximum allowed retries to acquire the lock. The default value is ``3`` times.
- ``retry_delay_min`` and ``retry_delay_max``: Float values representing waiting time (in seconds) before the next retry attempt. The default values are ``0.1`` and ``0.3``, respectively.

In order to acquire the lock, the ``lock`` function should be called. If the lock operation is successful, ``lock.valid`` will be true, if lock is not acquired then LockError will be raised.

From that moment, the lock is valid until the ``unlock`` function is called or when the ``lock_timeout`` is reached.

Call ``extend`` function to extend lock's lifetime for more ``lock_timeout`` interval.

Use ``is_locked`` to check if resource locked by another redlock instance.

In order to clear all the connections with Redis, the lock_manager ``destroy`` method can be called.

To-do
-----

* Expire the lock valid attribute according to the lock validity in a safe way if possible

.. _redlock: https://redis.io/topics/distlock
.. _Redis: https://redis.io
.. _asyncio: https://docs.python.org/3/library/asyncio.html
