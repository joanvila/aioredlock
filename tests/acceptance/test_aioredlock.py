import asyncio
import uuid

import aioredis
import asynctest
import pytest

from aioredlock import Aioredlock, LockError


@pytest.fixture
def redis_one_connection():
    return [{'host': 'localhost', 'port': 6379}]


@pytest.fixture
def redis_two_connections():
    return [
        {'host': 'localhost', 'port': 6379, 'db': 0},
        {'host': 'localhost', 'port': 6379, 'db': 1}
    ]


class TestAioredlock:

    async def check_simple_lock(self, lock_manager):
        resource = str(uuid.uuid4())

        assert await lock_manager.is_locked(resource) is False

        lock = await lock_manager.lock(resource)
        assert lock.valid is True
        assert await lock_manager.is_locked(resource) is True

        await lock_manager.extend(lock)
        assert lock.valid is True
        assert await lock_manager.is_locked(resource) is True

        await lock_manager.unlock(lock)
        assert lock.valid is False
        assert await lock_manager.is_locked(resource) is False

        await lock_manager.destroy()

    async def check_two_locks_on_different_resources(self, lock_manager):
        resource1 = str(uuid.uuid4())
        resource2 = str(uuid.uuid4())

        assert await lock_manager.is_locked(resource1) is False
        assert await lock_manager.is_locked(resource2) is False

        lock1 = await lock_manager.lock(resource1)
        assert lock1.valid is True
        assert await lock_manager.is_locked(resource1) is True
        assert await lock_manager.is_locked(resource2) is False

        lock2 = await lock_manager.lock(resource2)
        assert lock2.valid is True
        assert await lock_manager.is_locked(resource1) is True
        assert await lock_manager.is_locked(resource2) is True

        await lock_manager.unlock(lock1)
        assert lock1.valid is False
        assert await lock_manager.is_locked(resource1) is False
        await lock_manager.unlock(lock2)
        assert lock2.valid is False
        assert await lock_manager.is_locked(resource2) is False

        await lock_manager.destroy()

    async def check_two_locks_on_same_resource(self, lock_manager):
        resource = str(uuid.uuid4())

        assert await lock_manager.is_locked(resource) is False

        lock1 = await lock_manager.lock(resource)
        assert lock1.valid is True
        assert await lock_manager.is_locked(resource) is True

        with pytest.raises(LockError):
            await lock_manager.lock(resource)
        assert await lock_manager.is_locked(resource) is True

        await lock_manager.unlock(lock1)
        assert lock1.valid is False
        assert await lock_manager.is_locked(resource) is False

        await asyncio.sleep(0.2)  # wait for lock cleanup

        await lock_manager.destroy()

    @pytest.mark.asyncio
    async def test_simple_aioredlock_one_instance(
            self,
            redis_one_connection):

        await self.check_simple_lock(Aioredlock(redis_one_connection))

    @pytest.mark.asyncio
    async def test_aioredlock_two_locks_on_different_resources_one_instance(
            self,
            redis_one_connection):

        await self.check_two_locks_on_different_resources(Aioredlock(redis_one_connection))

    @pytest.mark.asyncio
    async def test_aioredlock_two_locks_on_same_resource_one_instance(
            self,
            redis_one_connection):

        await self.check_two_locks_on_same_resource(Aioredlock(redis_one_connection))

    @pytest.mark.asyncio
    async def test_simple_aioredlock_two_instances(
            self,
            redis_two_connections):

        await self.check_simple_lock(Aioredlock(redis_two_connections))

    @pytest.mark.asyncio
    async def test_aioredlock_two_locks_on_different_resources_two_instances(
            self,
            redis_two_connections):

        await self.check_two_locks_on_different_resources(Aioredlock(redis_two_connections))

    @pytest.mark.asyncio
    async def test_aioredlock_two_locks_on_same_resource_two_instances(
            self,
            redis_two_connections):

        await self.check_two_locks_on_same_resource(Aioredlock(redis_two_connections))

    @pytest.mark.asyncio
    async def test_aioredlock_lock_with_first_failed_try_two_instances(
        self,
        redis_two_connections
    ):

        lock_manager = Aioredlock(redis_two_connections)
        resource = str(uuid.uuid4())
        garbage_value = 'garbage'

        first_redis = await aioredis.create_redis(
            (redis_two_connections[0]['host'],
             redis_two_connections[0]['port'])
        )

        # write garbage to resource key in first instance
        await first_redis.set(resource, garbage_value)
        is_garbage = True

        # this patched sleep function will remove garbage from
        # frist instance before second try
        real_sleep = asyncio.sleep

        async def fake_sleep(delay):

            nonlocal is_garbage

            # remove garbage on sleep
            if is_garbage:
                await first_redis.delete(resource)
                is_garbage = False

            # print('fake_sleep(%s), value %s' % (delay, value))
            await real_sleep(delay)

        # here we will try to lock while first redis instance still have
        # resource key occupied by garbage
        # but just before second attempt patched asyncio.sleep() function
        # will clean up garbage key to let lock be acquired
        with asynctest.patch("asyncio.sleep", fake_sleep):
            lock = await lock_manager.lock(resource)
        assert lock.valid is True

        await lock_manager.unlock(lock)
        assert lock.valid is False

        await lock_manager.destroy()
        first_redis.close()
        await first_redis.wait_closed()
