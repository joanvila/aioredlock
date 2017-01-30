import asyncio
import aioredis

from aioredlock.lock import Lock


class Aioredlock:

    LOCK_TIMEOUT = 10  # seconds

    def __init__(self, host='localhost', port=6379):
        self.redis_host = host
        self.redis_port = port

        self._pool = None

    async def lock(self, resource):
        """
        Tries to acquire de lock.
        If the lock is correctly acquired, the valid property of the lock is true.

        :return: :class:`aioredlock.Lock`
        """
        with await self._connect() as redis:
            await redis.set(resource, "my_random_value")
            await redis.expire(resource, self.LOCK_TIMEOUT)

        valid = True
        return Lock(resource, valid=valid)

    async def _connect(self):
        if self._pool is None:
            async with asyncio.Lock():
                if self._pool is None:
                    self._pool = await aioredis.create_pool(
                        (self.redis_host, self.redis_port), minsize=5)

        return (await self._pool)

    async def destroy(self):
        self._pool.close()
        await self._pool.wait_closed()
