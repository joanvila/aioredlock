import pytest
import uuid
from aioredlock import Aioredlock


class TestAioredlock:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("redis_connections", [
        ([{'host': 'localhost', 'port': 6379}]),
        ([
            {'host': 'localhost', 'port': 6379},
            {'host': 'localhost', 'port': 6378}
        ]),
    ])
    async def test_simple_aioredlock(self, redis_connections):
        resource = str(uuid.uuid4())
        lock_manager = Aioredlock(redis_connections)

        lock = await lock_manager.lock(resource)
        assert lock.valid is True

        await lock_manager.unlock(lock)
        assert lock.valid is False

        await lock_manager.destroy()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("redis_connections", [
        ([{'host': 'localhost', 'port': 6379}]),
        ([
            {'host': 'localhost', 'port': 6379},
            {'host': 'localhost', 'port': 6378}
        ]),
    ])
    async def test_aioredlock_two_locks_on_different_resources(self, redis_connections):
        resource1 = str(uuid.uuid4())
        resource2 = str(uuid.uuid4())
        lock_manager = Aioredlock(redis_connections)

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
    @pytest.mark.parametrize("redis_connections", [
        ([{'host': 'localhost', 'port': 6379}]),
        ([
            {'host': 'localhost', 'port': 6379},
            {'host': 'localhost', 'port': 6378}
        ]),
    ])
    async def test_aioredlock_two_locks_on_same_resource(self, redis_connections):
        resource = str(uuid.uuid4())
        lock_manager = Aioredlock(redis_connections)

        lock1 = await lock_manager.lock(resource)
        assert lock1.valid is True

        lock2 = await lock_manager.lock(resource)
        assert lock2.valid is False

        await lock_manager.unlock(lock1)
        assert lock1.valid is False

        await lock_manager.destroy()
