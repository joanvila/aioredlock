import pytest
from aioredlock import Aioredlock


class TestAioredlock:

    RESOURCE1 = "1"
    RESOURCE2 = "2"

    @pytest.mark.asyncio
    async def test_simple_aioredlock(self, redis_connection):
        lock_manager = Aioredlock(redis_connection.host, redis_connection.port)

        lock = await lock_manager.lock(self.RESOURCE1)
        assert lock.valid is True

        await lock_manager.unlock(lock)
        assert lock.valid is False

        await lock_manager.destroy()

    @pytest.mark.asyncio
    async def test_aioredlock_two_locks_on_different_resources(self, redis_connection):
        lock_manager = Aioredlock(redis_connection.host, redis_connection.port)

        lock1 = await lock_manager.lock(self.RESOURCE1)
        assert lock1.valid is True

        lock2 = await lock_manager.lock(self.RESOURCE2)
        assert lock2.valid is True

        await lock_manager.unlock(lock1)
        assert lock1.valid is False
        await lock_manager.unlock(lock2)
        assert lock2.valid is False

        await lock_manager.destroy()

    @pytest.mark.asyncio
    async def test_aioredlock_two_locks_on_same_resource(self, redis_connection):
        lock_manager = Aioredlock(redis_connection.host, redis_connection.port)

        lock1 = await lock_manager.lock(self.RESOURCE1)
        assert lock1.valid is True

        lock2 = await lock_manager.lock(self.RESOURCE1)
        assert lock2.valid is False

        await lock_manager.unlock(lock1)
        assert lock1.valid is False

        await lock_manager.destroy()
