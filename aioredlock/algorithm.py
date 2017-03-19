import asyncio
import uuid
import random

from aioredlock.redis import Redis
from aioredlock.lock import Lock


class Aioredlock:

    LOCK_TIMEOUT = 10000  # 10 seconds

    UNLOCK_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end"""

    retry_count = 3
    retry_delay_min = 0.1
    retry_delay_max = 0.3

    def __init__(self, redis_connections=[{'host': 'localhost', 'port': 6379}]):
        """
        Initializes Aioredlock with the list of redis instances

        :param redis_connections: A list of dicts like:
        [{"host": "localhost", "port": 6379}]
        """

        self.redis = Redis(redis_connections, self.LOCK_TIMEOUT)

        # Proportional drift time to the length of the lock
        # See https://redis.io/topics/distlock#is-the-algorithm-asynchronous for more info
        self.drift = int(self.LOCK_TIMEOUT * 0.01) + 2

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

        return Lock(resource, lock_identifier, valid=valid_lock)

    def _retry_delay(self):
        return random.uniform(self.retry_delay_min, self.retry_delay_max)

    def _valid_lock(self, locked, elapsed_time):
        return locked and int(self.LOCK_TIMEOUT - elapsed_time - self.drift) > 0

    async def unlock(self, lock):
        """
        Release the lock and sets it's validity to False.

        :param lock: :class:`aioredlock.Lock`
        """
        await self.redis.run_lua(self.UNLOCK_SCRIPT, keys=[lock.resource], args=[lock.id])

        lock.valid = False

    async def destroy(self):
        """
        Clear all the redis connections
        """
        await self.redis.clear_connections()
