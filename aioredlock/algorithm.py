import asyncio
import random
import uuid

import attr

from aioredlock.lock import Lock
from aioredlock.redis import Redis


def validate_lock_timeout(instance, attribute, value):
    """
    Validate if lock_timeout is greater than 0
    """
    if value <= 0:
        raise ValueError("Lock timeout must be greater than 0 ms.")


@attr.s
class Aioredlock:
    redis_connections = attr.ib(default=[{'host': 'localhost', 'port': 6379}])
    lock_timeout = attr.ib(default=10000, convert=int, validator=validate_lock_timeout)
    # Proportional drift time to the length of the lock
    # See https://redis.io/topics/distlock#is-the-algorithm-asynchronous for more info
    drift = attr.ib(default=attr.Factory(
        lambda self: int(self.lock_timeout * 0.01) + 2, takes_self=True
    ), convert=int)
    retry_count = attr.ib(default=3, convert=int)
    retry_delay_min = attr.ib(default=0.1, convert=float)
    retry_delay_max = attr.ib(default=0.3, convert=float)
    UNLOCK_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end"""

    def __attrs_post_init__(self):
        self.redis = Redis(self.redis_connections, self.lock_timeout)

    async def lock(self, resource):
        """
        Tries to acquire de lock.
        If the lock is correctly acquired, the valid property of the returned lock is True.

        :param resource: The string identifier of the resource to lock
        :return: :class:`aioredlock.Lock`
        """
        retries = 1
        lock_identifier = str(uuid.uuid4())

        locked, elapsed_time = await self.redis.set_lock(resource, lock_identifier)
        valid_lock = self._valid_lock(locked, elapsed_time)

        while not valid_lock and retries < self.retry_count:  # retry policy
            await asyncio.sleep(self._retry_delay())
            locked, elapsed_time = await self.redis.set_lock(resource, lock_identifier)
            valid_lock = self._valid_lock(locked, elapsed_time)
            retries += 1

        lock = Lock(resource, lock_identifier, valid=valid_lock)

        # try to clean up in case of fault
        if not valid_lock:
            await self.unlock(lock)

        return lock

    def _retry_delay(self):
        return random.uniform(self.retry_delay_min, self.retry_delay_max)

    def _valid_lock(self, locked, elapsed_time):
        return locked and int(self.lock_timeout - elapsed_time - self.drift) > 0

    async def unlock(self, lock):
        """
        Release the lock and sets it's validity to False if
        lock successfuly released.

        :param lock: :class:`aioredlock.Lock`
        """
        unlocked, elapsed_time = (
            await self.redis.unset_lock(lock.resource, lock.id))

        lock.valid = lock.valid and not unlocked

    async def destroy(self):
        """
        Clear all the redis connections
        """
        await self.redis.clear_connections()
