import asyncio
import logging
import re
import time
from distutils.version import StrictVersion
from itertools import groupby

import aioredis

from aioredlock.errors import LockError
from aioredlock.sentinel import Sentinel
from aioredlock.utility import clean_password


def all_equal(iterable):
    """Returns True if all the elements are equal to each other"""
    g = groupby(iterable)
    return next(g, True) and not next(g, False)


class Instance:

    # KEYS[1] - lock resource key
    # ARGS[1] - lock unique identifier
    # ARGS[2] - expiration time in milliseconds
    SET_LOCK_SCRIPT = """
    local identifier = redis.call('get', KEYS[1])
    if not identifier or identifier == ARGV[1] then
        return redis.call("set", KEYS[1], ARGV[1], 'PX', ARGV[2])
    else
        return redis.error_reply('ERROR')
    end"""

    # KEYS[1] - lock resource key
    # ARGS[1] - lock unique identifier
    UNSET_LOCK_SCRIPT = """
    local identifier = redis.call('get', KEYS[1])
    if not identifier then
        return redis.status_reply('OK')
    elseif identifier == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return redis.error_reply('ERROR')
    end"""

    # KEYS[1] - lock resource key
    GET_LOCK_TTL_SCRIPT = """
    local identifier = redis.call('get', KEYS[1])
    if not identifier then
        return redis.error_reply('ERROR')
    elseif identifier == ARGV[1] then
        return redis.call("TTL", KEYS[1])
    else
        return redis.error_reply('ERROR')
    end"""

    def __init__(self, connection):
        """
        Redis instance constructor

        Constructor takes single argument - a redis host address
        The address can be one of the following:
         * a dict - {'host': 'localhost', 'port': 6379,
                     'db': 0, 'password': 'pass'}
           all keys except host and port will be passed as kwargs to
           the aioredis.create_redis_pool();
         * an aioredlock.redis.Sentinel object;
         * a Redis URI - "redis://host:6379/0?encoding=utf-8";
         * a (host, port) tuple - ('localhost', 6379);
         * or a unix domain socket path string - "/path/to/redis.sock".
         * a redis connection pool.

        :param connection: redis host address (dict, tuple or str)
        """

        self.connection = connection

        self._pool = None
        self._lock = asyncio.Lock()

        self.set_lock_script_sha1 = None
        self.unset_lock_script_sha1 = None
        self.get_lock_ttl_script_sha1 = None

    @property
    def log(self):
        return logging.getLogger(__name__)

    def __repr__(self):
        connection_details = clean_password(self.connection)
        return "<%s(connection='%s'>" % (self.__class__.__name__, connection_details)

    @staticmethod
    async def _create_redis_pool(*args, **kwargs):
        """
        Adapter to support both aioredis-0.3.0 and aioredis-1.0.0
        For aioredis-1.0.0 and later calls:
            aioredis.create_redis_pool(*args, **kwargs)
        For aioredis-0.3.0 calls:
            aioredis.create_pool(*args, **kwargs)
        """

        if StrictVersion(aioredis.__version__) >= StrictVersion('1.0.0'):  # pragma no cover
            return await aioredis.create_redis_pool(*args, **kwargs)
        else:  # pragma no cover
            return await aioredis.create_pool(*args, **kwargs)

    async def _register_scripts(self, redis):
        tasks = []
        for script in [
                self.SET_LOCK_SCRIPT,
                self.UNSET_LOCK_SCRIPT,
                self.GET_LOCK_TTL_SCRIPT,
        ]:
            script = re.sub(r'^\s+', '', script, flags=re.M).strip()
            tasks.append(redis.script_load(script))
        (
            self.set_lock_script_sha1,
            self.unset_lock_script_sha1,
            self.get_lock_ttl_script_sha1,
        ) = (r.decode() for r in await asyncio.gather(*tasks))

    async def connect(self):
        """
        Get an connection for the self instance
        """
        address, redis_kwargs = (), {}

        if isinstance(self.connection, Sentinel):
            self._pool = await self.connection.get_master()
        elif isinstance(self.connection, dict):
            # a dict like {'host': 'localhost', 'port': 6379,
            #              'db': 0, 'password': 'pass'}
            kwargs = self.connection.copy()
            address = (
                kwargs.pop('host', 'localhost'),
                kwargs.pop('port', 6379)
            )
            redis_kwargs = kwargs
        elif isinstance(self.connection, aioredis.Redis):
            self._pool = self.connection
        else:
            # a tuple or list ('localhost', 6379)
            # a string "redis://host:6379/0?encoding=utf-8" or
            # a unix domain socket path "/path/to/redis.sock"
            address = self.connection

        if self._pool is None:
            if 'minsize' not in redis_kwargs:
                redis_kwargs['minsize'] = 1
            if 'maxsize' not in redis_kwargs:
                redis_kwargs['maxsize'] = 100
            async with self._lock:
                if self._pool is None:
                    self.log.debug('Connecting %s', repr(self))
                    self._pool = await self._create_redis_pool(address, **redis_kwargs)

        if self.set_lock_script_sha1 is None or self.unset_lock_script_sha1 is None:
            with await self._pool as redis:
                await self._register_scripts(redis)

        return await self._pool

    async def close(self):
        """
        Closes connection and resets pool
        """
        if self._pool is not None and not isinstance(self.connection, aioredis.Redis):
            self._pool.close()
            await self._pool.wait_closed()
        self._pool = None

    async def set_lock(self, resource, lock_identifier, lock_timeout, register_scripts=False):
        """
        Lock this instance and set lock expiration time to lock_timeout
        :param resource: redis key to set
        :param lock_identifier: uniquie id of lock
        :param lock_timeout: timeout for lock in seconds
        :raises: LockError if lock is not acquired
        """

        lock_timeout_ms = int(lock_timeout * 1000)

        try:
            with await self.connect() as redis:
                if register_scripts is True:
                    await self._register_scripts(redis)
                await redis.evalsha(
                    self.set_lock_script_sha1,
                    keys=[resource],
                    args=[lock_identifier, lock_timeout_ms]
                )
        except aioredis.errors.ReplyError as exc:  # script fault
            if exc.args[0].startswith('NOSCRIPT'):
                return await self.set_lock(resource, lock_identifier, lock_timeout, register_scripts=True)
            self.log.debug('Can not set lock "%s" on %s',
                           resource, repr(self))
            raise LockError('Can not set lock') from exc
        except (aioredis.errors.RedisError, OSError) as exc:
            self.log.error('Can not set lock "%s" on %s: %s',
                           resource, repr(self), repr(exc))
            raise LockError('Can not set lock') from exc
        except asyncio.CancelledError:
            self.log.debug('Lock "%s" is cancelled on %s',
                           resource, repr(self))
            raise
        except Exception:
            self.log.exception('Can not set lock "%s" on %s',
                               resource, repr(self))
            raise
        else:
            self.log.debug('Lock "%s" is set on %s', resource, repr(self))

    async def get_lock_ttl(self, resource, lock_identifier, register_scripts=False):
        """
        Fetch this instance and set lock expiration time to lock_timeout
        :param resource: redis key to get
        :param lock_identifier: unique id of the lock to get
        :param register_scripts: register redis, usually already done, so 'False'.
        :raises: LockError if lock is not available
        """
        try:
            with await self.connect() as redis:
                if register_scripts is True:
                    await self._register_scripts(redis)
                ttl = await redis.evalsha(
                    self.get_lock_ttl_script_sha1,
                    keys=[resource],
                    args=[lock_identifier]
                )
        except aioredis.errors.ReplyError as exc:  # script fault
            if exc.args[0].startswith('NOSCRIPT'):
                return await self.get_lock_ttl(resource, lock_identifier, register_scripts=True)
            self.log.debug('Can not get lock "%s" on %s',
                           resource, repr(self))
            raise LockError('Can not get lock') from exc
        except (aioredis.errors.RedisError, OSError) as exc:
            self.log.error('Can not get lock "%s" on %s: %s',
                           resource, repr(self), repr(exc))
            raise LockError('Can not get lock') from exc
        except asyncio.CancelledError:
            self.log.debug('Lock "%s" is cancelled on %s',
                           resource, repr(self))
            raise
        except Exception:
            self.log.exception('Can not get lock "%s" on %s',
                               resource, repr(self))
            raise
        else:
            self.log.debug('Lock "%s" with TTL %s is on %s', resource, ttl, repr(self))
            return ttl

    async def unset_lock(self, resource, lock_identifier, register_scripts=False):
        """
        Unlock this instance
        :param resource: redis key to set
        :param lock_identifier: uniquie id of lock
        :raises: LockError if the lock resource acquired with different lock_identifier
        """
        try:
            with await self.connect() as redis:
                if register_scripts is True:
                    await self._register_scripts(redis)
                await redis.evalsha(
                    self.unset_lock_script_sha1,
                    keys=[resource],
                    args=[lock_identifier]
                )
        except aioredis.errors.ReplyError as exc:  # script fault
            if exc.args[0].startswith('NOSCRIPT'):
                return await self.unset_lock(resource, lock_identifier, register_scripts=True)
            self.log.debug('Can not unset lock "%s" on %s',
                           resource, repr(self))
            raise LockError('Can not unset lock') from exc
        except (aioredis.errors.RedisError, OSError) as exc:
            self.log.error('Can not unset lock "%s" on %s: %s',
                           resource, repr(self), repr(exc))
            raise LockError('Can not set lock') from exc
        except asyncio.CancelledError:
            self.log.debug('Lock "%s" unset is cancelled on %s',
                           resource, repr(self))
            raise
        except Exception:
            self.log.exception('Can not unset lock "%s" on %s',
                               resource, repr(self))
            raise
        else:
            self.log.debug('Lock "%s" is unset on %s', resource, repr(self))

    async def is_locked(self, resource):
        """
        Checks if the resource is locked by any redlock instance.

        :param resource: The resource string name to check
        :returns: True if locked else False
        """

        with await self.connect() as redis:
            lock_identifier = await redis.get(resource)
        if lock_identifier:
            return True
        else:
            return False


class Redis:

    def __init__(self, redis_connections):

        self.instances = []
        for connection in redis_connections:
            self.instances.append(Instance(connection))

    @property
    def log(self):
        return logging.getLogger(__name__)

    async def set_lock(self, resource, lock_identifier, lock_timeout=10.0):
        """
        Tries to set the lock to all the redis instances

        :param resource: The resource string name to lock
        :param lock_identifier: The id of the lock. A unique string
        :param lock_timeout: lock's lifetime
        :return float: The elapsed time that took to lock the instances
            in seconds
        :raises: LockError if the lock has not been set to at least (N/2 + 1)
            instances
        """
        start_time = time.monotonic()

        successes = await asyncio.gather(*[
            i.set_lock(resource, lock_identifier, lock_timeout) for
            i in self.instances
        ], return_exceptions=True)
        successful_sets = sum(s is None for s in successes)

        elapsed_time = time.monotonic() - start_time
        locked = True if successful_sets >= int(len(self.instances) / 2) + 1 else False

        self.log.debug('Lock "%s" is set on %d/%d instances in %s seconds',
                       resource, successful_sets, len(self.instances), elapsed_time)

        if not locked:
            raise LockError('Can not acquire the lock "%s"' % resource)

        return elapsed_time

    async def get_lock_ttl(self, resource, lock_identifier=None):
        """
        Tries to get the lock from all the redis instances

        :param resource: The resource string name to fetch
        :param lock_identifier: The id of the lock. A unique string
        :return float: The TTL of that lock reported by redis
        :raises: LockError if the lock has not been set to at least (N/2 + 1)
            instances
        """
        start_time = time.monotonic()
        successes = await asyncio.gather(*[
            i.get_lock_ttl(resource, lock_identifier) for
            i in self.instances
        ], return_exceptions=True)
        successful_list = [s for s in successes if not isinstance(s, Exception)]
        # should check if all the value are approx. the same with math.isclose...
        locked = True if len(successful_list) >= int(len(self.instances) / 2) + 1 else False
        success = all_equal(successful_list) and locked
        elapsed_time = time.monotonic() - start_time

        self.log.debug('Lock "%s" is set on %d/%d instances in %s seconds',
                       resource, len(successful_list), len(self.instances), elapsed_time)

        if not success:
            raise LockError('Could not fetch the TTL for lock "%s"' % resource)

        return successful_list[0]

    async def unset_lock(self, resource, lock_identifier):
        """
        Tries to unset the lock to all the redis instances

        :param resource: The resource string name to lock
        :param lock_identifier: The id of the lock. A unique string
        :return float: The elapsed time that took to lock the instances in iseconds
        :raises: LockError if the lock has not matching identifier in more then
            (N/2 - 1) instances
        """
        start_time = time.monotonic()

        successes = await asyncio.gather(*[
            i.unset_lock(resource, lock_identifier) for
            i in self.instances
        ], return_exceptions=True)
        successful_removes = sum(s is None for s in successes)

        elapsed_time = time.monotonic() - start_time
        unlocked = True if successful_removes >= int(len(self.instances) / 2) + 1 else False

        self.log.debug('Lock "%s" is unset on %d/%d instances in %s seconds',
                       resource, successful_removes, len(self.instances), elapsed_time)

        if not unlocked:
            raise LockError('Can not release the lock')

        return elapsed_time

    async def is_locked(self, resource):
        """
        Checks if the resource is locked by any redlock instance.

        :param resource: The resource string name to lock
        :returns: True if locked else False
        """

        successes = await asyncio.gather(*[
            i.is_locked(resource) for
            i in self.instances
        ], return_exceptions=True)
        successful_sets = sum(s is True for s in successes)

        locked = True if successful_sets >= int(len(self.instances) / 2) + 1 else False

        return locked

    async def clear_connections(self):

        self.log.debug('Clearing connection')

        if self.instances:
            coros = []
            while self.instances:
                coros.append(self.instances.pop().close())
            await asyncio.gather(*(coros))
