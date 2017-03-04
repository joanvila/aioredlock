import asyncio
import aioredis
import time


class Instance:

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self._pool = None

    async def connect(self):
        """
        Get an connection for the self instance
        """
        if self._pool is None:
            async with asyncio.Lock():
                if self._pool is None:
                    self._pool = await aioredis.create_pool(
                        (self.host, self.port), minsize=5)

        return await self._pool


class Redis:

    def __init__(self, redis_connections, lock_timeout):

        self.instances = []
        for connection in redis_connections:
            self.instances.append(
                Instance(connection['host'], connection['port']))

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
        locked = True if successful_sets >= int(len(self.instances)/2) + 1 else False

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
