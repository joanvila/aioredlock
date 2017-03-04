import pytest
import uuid
from aioredlock import Aioredlock


class TestAioredlock:

    @pytest.mark.asyncio
    async def test_simple_aioredlock(self, single_redis_connection):
        resource = str(uuid.uuid4())
        lock_manager = Aioredlock(single_redis_connection)

        lock = await lock_manager.lock(resource)
        assert lock.valid is True

        await lock_manager.unlock(lock)
        assert lock.valid is False

        await lock_manager.destroy()

    @pytest.mark.asyncio
    async def test_aioredlock_two_locks_on_different_resources(self, single_redis_connection):
        resource1 = str(uuid.uuid4())
        resource2 = str(uuid.uuid4())
        lock_manager = Aioredlock(single_redis_connection)

        lock1 = await lock_manager.lock(resource1)
        assert lock1.valid is True

        lock2 = await lock_manager.lock(resource2)
        assert lock2.valid is True

        await lock_manager.unlock(lock1)
        assert lock1.valid is False
        await lock_manager.unlock(lock2)
        assert lock2.valid is False

        await lock_manager.destroy()

    @pytest.mark.asyncio
    async def test_aioredlock_two_locks_on_same_resource(self, single_redis_connection):
        resource = str(uuid.uuid4())
        lock_manager = Aioredlock(single_redis_connection)

        lock1 = await lock_manager.lock(resource)
        assert lock1.valid is True

        lock2 = await lock_manager.lock(resource)
        assert lock2.valid is False

        await lock_manager.unlock(lock1)
        assert lock1.valid is False

        await lock_manager.destroy()
