import asyncio
import contextlib
import logging
import random
import uuid

import attr

from aioredlock.errors import LockError
from aioredlock.lock import Lock
from aioredlock.redis import Redis
from aioredlock.utility import clean_password


@attr.s
class Aioredlock:
    redis_connections = attr.ib(
        default=[{"host": "localhost", "port": 6379}], repr=clean_password
    )
    retry_count = attr.ib(default=3, converter=int)
    retry_delay_min = attr.ib(default=0.1, converter=float)
    retry_delay_max = attr.ib(default=0.3, converter=float)
    internal_lock_timeout = attr.ib(default=10.0, converter=float)

    def __attrs_post_init__(self):
        self.redis = Redis(self.redis_connections)
        self._watchdogs = {}
        self._locks = {}

    @retry_count.validator
    def _validate_retry_count(self, attribute, value):
        """
        Validate if retry_count is greater or equal 1
        """
        if value < 1:
            raise ValueError("Retry count must be greater or equal 1.")

    @internal_lock_timeout.validator
    def _validate_internal_lock_timeout(self, attribute, value):
        """
        Validate if internal_lock_timeout is greater than 0
        """
        if value <= 0:
            raise ValueError("Internal lock_timeout must be greater than 0 seconds.")

    @retry_delay_min.validator
    @retry_delay_max.validator
    def _validate_retry_delay(self, attribute, value):
        """
        Validate if retry_delay_min and retry_delay_max is greater than 0
        """
        if value <= 0:
            raise ValueError("Retry delay must be greater than 0 seconds.")

    @property
    def log(self):
        return logging.getLogger(__name__)

    async def _set_lock(self, resource, lock_identifier, lease_time):

        error = RuntimeError('Retry count less then one')

        # Proportional drift time to the length of the lock
        # See https://redis.io/topics/distlock#is-the-algorithm-asynchronous for more info
        drift = lease_time * 0.01 + 0.002

        try:
            # global try/except to catch CancelledError
            for n in range(self.retry_count):
                self.log.debug('Acquiring lock "%s" try %d/%d',
                               resource, n + 1, self.retry_count)
                if n != 0:
                    delay = random.uniform(self.retry_delay_min,
                                           self.retry_delay_max)
                    await asyncio.sleep(delay)
                try:
                    elapsed_time = await self.redis.set_lock(resource, lock_identifier, lease_time)
                except LockError as exc:
                    error = exc
                    continue

                if lease_time - elapsed_time - drift <= 0:
                    error = LockError('Lock timeout')
                    self.log.debug('Timeout in acquiring the lock "%s"',
                                   resource)
                    continue

                error = None
                break
            else:
                # break never reached
                raise error

        except (Exception, asyncio.CancelledError):
            # cleanup in case of fault or cancellation will run in background
            async def cleanup():
                self.log.debug('Cleaning up lock "%s"', resource)
                with contextlib.suppress(LockError):
                    await self.redis.unset_lock(resource, lock_identifier)

            asyncio.ensure_future(cleanup())

            raise

    async def _auto_extend(self, lock):
        """
        Tries to reset the lock's lifetime to lock_timeout every 0.6*lock_timeout automatically
        In case of fault the LockError exception will be raised
        :param lock: :class:`aioredlock.Lock`
        :raises: LockError in case of fault
        """

        await asyncio.sleep(0.6 * self.internal_lock_timeout)
        try:
            await self.extend(lock)
        except Exception:
            self.log.debug('Error in extending the lock "%s"',
                           lock.resource)

        self._watchdogs[lock.resource] = asyncio.ensure_future(self._auto_extend(lock))

    async def lock(self, resource, lock_timeout=None):
        """
        Tries to acquire the lock.
        If the lock is correctly acquired, the valid property of
        the returned lock is True.
        In case of fault the LockError exception will be raised

        :param resource: The string identifier of the resource to lock
        :param lock_timeout: Lock's lifetime
        :return: :class:`aioredlock.Lock`
        :raises: LockError in case of fault
        """
        lock_identifier = str(uuid.uuid4())

        if lock_timeout is not None and lock_timeout <= 0:
            raise ValueError("Lock timeout must be greater than 0 seconds.")

        lease_time = lock_timeout or self.internal_lock_timeout

        await self._set_lock(resource, lock_identifier, lease_time)

        lock = Lock(self, resource, lock_identifier, lock_timeout, valid=True)
        if lock_timeout is None:
            self._watchdogs[lock.resource] = asyncio.ensure_future(self._auto_extend(lock))
        self._locks[resource] = lock

        return lock

    async def extend(self, lock, lock_timeout=None):
        """
        Tries to reset the lock's lifetime to lock_timeout
        In case of fault the LockError exception will be raised

        :param lock: :class:`aioredlock.Lock`
        :param lock_timeout: extend lock's life time to lock_timeout
        :raises: RuntimeError if lock is not valid
        :raises: LockError in case of fault
        """
        self.log.debug('Extending lock "%s"', lock.resource)

        if not lock.valid:
            raise RuntimeError('Lock is not valid')
        if lock_timeout is not None and lock_timeout <= 0:
            raise ValueError("Lock timeout must be greater than 0 seconds.")

        new_lease_time = lock_timeout or lock.lock_timeout or self.internal_lock_timeout

        try:
            await self._set_lock(lock.resource, lock.id, new_lease_time)
        except Exception:
            with contextlib.suppress(LockError):
                await self.unlock(lock)
            raise

    async def unlock(self, lock):
        """
        Release the lock and sets it's validity to False if
        lock successfully released.
        In case of fault the LockError exception will be raised
        :param lock: :class:`aioredlock.Lock`
        :raises: LockError in case of fault
        """
        self.log.debug('Releasing lock "%s"', lock.resource)

        lock.valid = False

        if lock.resource in self._watchdogs:
            self._watchdogs[lock.resource].cancel()

            done, _ = await asyncio.wait([self._watchdogs[lock.resource]])
            for fut in done:
                try:
                    await fut
                except asyncio.CancelledError:
                    pass
                except Exception:
                    self.log.exception('Can not unlock "%s"', lock.resource)

            self._watchdogs.pop(lock.resource)

        await self.redis.unset_lock(lock.resource, lock.id)
        # raises LockError if can not unlock

    async def is_locked(self, resource_or_lock):
        """
        Checks if the resource or the lock is locked by any redlock instance.

        :param resource_or_lock: resource name or aioredlock.Lock instance
        :returns: True if locked else False
        """

        if isinstance(resource_or_lock, Lock):
            resource = resource_or_lock.resource
        elif isinstance(resource_or_lock, str):
            resource = resource_or_lock
        else:
            raise TypeError(
                'Argument should be ether aioredlock.Lock instance or string, '
                '%s is given.', type(resource_or_lock)
            )

        return await self.redis.is_locked(resource)

    async def destroy(self):
        """
        cancel all _watchdogs, unlock all locks and Clear all the redis connections
        """
        self.log.debug('Destroying %s', repr(self))

        for resource, lock in self._locks.items():
            if lock.valid:
                try:
                    await self.unlock(lock)
                except Exception:
                    self.log.exception('Can not unlock "%s"', resource)

        self._locks.clear()
        self._watchdogs.clear()

        await self.redis.clear_connections()

    async def get_active_locks(self):
        """
        Return all stored locks that are valid.

        .. note::
            This function is only really useful in learning if there are no
            active locks. It is possible that by the time the a lock is
            returned from this function that it is no longer active.
        """
        ret = []
        for lock in self._locks.values():
            if lock.valid is True and await lock.is_locked():
                ret.append(lock)
        return ret

    async def get_lock(self, resource, lock_identifier):
        """
        recreate a aioredlock.Lock from the goven params and the ttl from redis.
        so checks if the lock is valid somehow too...

        :param resource: The string identifier of the resource to lock
        :param lock_identifier: The identifier of the lock
        :return: a new `aioredlock.Lock`.
        """
        ttl = await self.redis.get_lock_ttl(resource, lock_identifier)
        lock = Lock(self, resource, lock_identifier, ttl, valid=True)
        return lock
