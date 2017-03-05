import pytest
import os
import uuid
from aioredlock import Aioredlock


@pytest.fixture
def redis_one_connection():
    return [{'host': 'localhost', 'port': 6379}]


@pytest.fixture
def redis_two_connections():
    return [
            {'host': 'localhost', 'port': 6379},
            {'host': 'localhost', 'port': 6378}
        ]


class TestAioredlock:

    async def check_simple_lock(self, lock_manager):
        resource = str(uuid.uuid4())

        lock = await lock_manager.lock(resource)
        assert lock.valid is True

        await lock_manager.unlock(lock)
        assert lock.valid is False

        await lock_manager.destroy()

    async def check_two_locks_on_different_resources(self, lock_manager):
        resource1 = str(uuid.uuid4())
        resource2 = str(uuid.uuid4())

        lock1 = await lock_manager.lock(resource1)
        assert lock1.valid is True

        lock2 = await lock_manager.lock(resource2)
        assert lock2.valid is True

        await lock_manager.unlock(lock1)
        assert lock1.valid is False
        await lock_manager.unlock(lock2)
        assert lock2.valid is False

        await lock_manager.destroy()

    async def check_two_locks_on_same_resource(self, lock_manager):
        resource = str(uuid.uuid4())

        lock1 = await lock_manager.lock(resource)
        assert lock1.valid is True

        lock2 = await lock_manager.lock(resource)
        assert lock2.valid is False

        await lock_manager.unlock(lock1)
        assert lock1.valid is False

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

    reason = 'CI only has one redis instance'

    @pytest.mark.asyncio
    @pytest.mark.skipif(os.getenv('CI'), reason=reason)
    async def test_simple_aioredlock_two_instances(
            self,
            redis_two_connections):

        await self.check_simple_lock(Aioredlock(redis_two_connections))

    @pytest.mark.asyncio
    @pytest.mark.skipif(os.getenv('CI'), reason=reason)
    async def test_aioredlock_two_locks_on_different_resources_two_instances(
            self,
            redis_two_connections):

        await self.check_two_locks_on_different_resources(Aioredlock(redis_two_connections))

    @pytest.mark.asyncio
    @pytest.mark.skipif(os.getenv('CI'), reason=reason)
    async def test_aioredlock_two_locks_on_same_resource_two_instances(
            self,
            redis_two_connections):

        await self.check_two_locks_on_same_resource(Aioredlock(redis_two_connections))
