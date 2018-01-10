import asyncio
import contextlib
import random
import uuid

from aioredlock.errors import LockError
from aioredlock.lock import Lock
from aioredlock.redis import Redis


class Aioredlock:

    LOCK_TIMEOUT = 10000  # 10 seconds

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
        If the lock is correctly acquired, the valid property of
        the returned lock is True.
        In case of fault LockError exception will be raised

        :param resource: The string identifier of the resource to lock
        :return: :class:`aioredlock.Lock`
        :raises: LockError in case of fault
        """
        lock_identifier = str(uuid.uuid4())
        error = RuntimeError('Retry count less then one')

        try:
            # global try/except to catch CancelledError
            for n in range(self.retry_count):
                if n != 0:
                    delay = random.uniform(self.retry_delay_min,
                                           self.retry_delay_max)
                    await asyncio.sleep(delay)
                try:
                    elapsed_time = \
                        await self.redis.set_lock(resource, lock_identifier)
                except LockError as exc:
                    error = exc
                    continue

                if int(self.LOCK_TIMEOUT - elapsed_time - self.drift) <= 0:
                    error = LockError('Lock timeout')
                    continue

                error = None
                break
            else:
                # break never reached
                raise error

        except Exception as exc:
            # cleanup in case of fault or cencellation will run in background
            async def cleanup():
                with contextlib.suppress(LockError):
                    await self.redis.unset_lock(resource, lock_identifier)

            asyncio.ensure_future(cleanup())

            raise

        return Lock(self, resource, lock_identifier, valid=True)

    async def extend(self, lock):
        """
        Tries to extend lock lifetime by lock_timeout
        Returns True if the lock is valid and lifetime correctly extended on
        more then half redis instances.
        Raises LockError if can not extend more then half of instances

        :param lock: :class:`aioredlock.Lock`
        :raises: RuntimeError if lock is not valid
        :raises: LockError in case of fault
        """

        if not lock.valid:
            raise RuntimeError('Lock is not valid')

        await self.redis.set_lock(lock.resource, lock.id)

    async def unlock(self, lock):
        """
        Release the lock and sets it's validity to False if
        lock successfuly released.

        :param lock: :class:`aioredlock.Lock`
        :raises: LockError in case of fault
        """
        await self.redis.unset_lock(lock.resource, lock.id)
        # raises LockError if can not unlock

        lock.valid = False

    async def destroy(self):
        """
        Clear all the redis connections
        """
        await self.redis.clear_connections()
