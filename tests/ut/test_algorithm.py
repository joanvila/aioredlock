import pytest

from asynctest import CoroutineMock
from unittest.mock import Mock, ANY

from aioredlock import Aioredlock
from aioredlock import Lock


@pytest.fixture
def locked_lock():
    return Lock("resource_name", 1, True)


class TestAioredlock:

    def test_default_initialization(self):
        lock_manager = Aioredlock()
        assert lock_manager.redis_host == 'localhost'
        assert lock_manager.redis_port == 6379
        assert lock_manager._pool is None

    def test_initialization_with_params(self):
        lock_manager = Aioredlock('host', 1)
        assert lock_manager.redis_host == 'host'
        assert lock_manager.redis_port == 1
        assert lock_manager._pool is None

    @pytest.mark.asyncio
    async def test_lock(self, lock_manager):
        lock_manager, pool = lock_manager
        lock = await lock_manager.lock("resource_name")

        pool.set.assert_called_once_with(
            "resource_name",
            ANY,
            pexpire=1,
            exist=pool.SET_IF_NOT_EXIST
        )

        assert lock.resource == "resource_name"
        assert lock.id == ANY
        assert lock.valid is True

    @pytest.mark.asyncio
    async def test_unlock(self, lock_manager, locked_lock):
        lock_manager, pool = lock_manager
        await lock_manager.unlock(locked_lock)

        pool.eval.assert_called_once_with(
            Aioredlock().UNLOCK_SCRIPT,
            keys=[locked_lock.resource],
            args=[locked_lock.id]
        )

        assert locked_lock.valid is False

    @pytest.mark.asyncio
    async def test_connect(self):
        assert True is True

    @pytest.mark.asyncio
    async def test_destroy_lock_manager(self):
        lock_manager = Aioredlock()

        lock_manager._pool = Mock()
        lock_manager._pool.close.return_value = True
        lock_manager._pool.wait_closed = CoroutineMock()

        await lock_manager.destroy()

        lock_manager._pool.close.assert_called_once_with()
        lock_manager._pool.wait_closed.assert_called_once_with()
