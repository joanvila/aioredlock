import asyncio
import time
from distutils.version import StrictVersion

import aioredis


class Instance:

    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password

        self._pool = None
        self._lock = asyncio.Lock()

    @staticmethod
    async def _create_redis_pool(*args, **kwargs):
        """
        Adaptor to support both aioredis-0.3.0 and aioredis-1.0.0
        For aioredis-1.0.0 and later calls:
            aioredis.create_redis_pool(*args, **kwargs)
        For aioredis-0.3.0 calls:
            aioredis.create_pool(*args, **kwargs)
        """

        if StrictVersion(aioredis.__version__) >= StrictVersion('1.0.0'):
            return await aioredis.create_redis_pool(*args, **kwargs)
        else:
            return await aioredis.create_pool(*args, **kwargs)

    async def connect(self):
        """
        Get an connection for the self instance
        """
        if self._pool is None:
            async with self._lock:
                if self._pool is None:
                    self._pool = await self._create_redis_pool(
                        (self.host, self.port),
                        db=self.db, password=self.password,
                        minsize=1, maxsize=100)

        return await self._pool


class Redis:

    def __init__(self, redis_connections, lock_timeout):

        self.instances = []
        for connection in redis_connections:
            self.instances.append(
                Instance(**connection))

        self.lock_timeout = lock_timeout

    async def set_lock(self, resource, lock_identifier):
        """
        Tries to set the lock to all the redis instances

        :param resource: The resource string name to lock
        :param lock_identifier: The id of the lock. A unique string
        :return tuple: A True boolean if the lock has been set to at least (N/2 + 1) instances
        or a False if not and the elapsed time that took to lock the instances
        """
        start_time = int(time.time() * 1000)
        successful_sets = 0

        # TODO: Ensure futures and gather them to lock the instances
        for instance in self.instances:
            success = await self._lock_single_instance(instance, resource, lock_identifier)
            if success:
                successful_sets += 1

        elapsed_time = int(time.time() * 1000) - start_time
        locked = True if successful_sets >= int(
            len(self.instances) / 2) + 1 else False

        return (locked, elapsed_time)

    async def _lock_single_instance(self, redis_instance, resource, lock_identifier):
        with await redis_instance.connect() as connection:
            return await connection.set(
                resource,
                lock_identifier,
                pexpire=self.lock_timeout,
                exist=connection.SET_IF_NOT_EXIST)

    async def run_lua(self, script, keys=[], args=[]):
        """
        Run the same lua script on all the redis instances
        """
        for instance in self.instances:
            with await instance.connect() as connection:
                await connection.eval(script, keys=keys, args=args)

    async def clear_connections(self):
        # TODO: Ensure futures and gather them to lock the instances
        for instance in self.instances:
            instance._pool.close()
            await instance._pool.wait_closed()
