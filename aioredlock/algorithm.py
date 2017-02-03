import asyncio
import aioredis
import uuid

from aioredlock.lock import Lock


class Aioredlock:

    LOCK_TIMEOUT = 10000  # 10 seconds

    UNLOCK_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end"""

    def __init__(self, host='localhost', port=6379):
        # TODO: Support for more that one redis instance

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
            lock_identifier = str(uuid.uuid4())
            valid_lock = await redis.set(
                resource, lock_identifier, pexpire=self.LOCK_TIMEOUT, exist=redis.SET_IF_NOT_EXIST)

            return Lock(resource, lock_identifier, valid=valid_lock)

    async def unlock(self, lock):
        """
        Release the lock and sets it's validity to false.

        :param lock: :class:`aioredlock.Lock`
        """
        with await self._connect() as redis:
            await redis.eval(self.UNLOCK_SCRIPT, keys=[lock.resource], args=[lock.id])

        lock.valid = False

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
