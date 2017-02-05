import pytest
import asynctest

from asynctest import CoroutineMock
from unittest.mock import Mock, ANY

from aioredlock import Aioredlock


@pytest.fixture
def mock_redis():

    class RedisMock:
        set = CoroutineMock()
        eval = CoroutineMock()

    redis = RedisMock()
    return redis


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
    async def test_lock_acquired_v1(self, mocker, mock_redis):
        mocker.patch("aioredis.create_pool", CoroutineMock(return_value=mock_redis))

        lock_manager = Aioredlock()
        lock_manager.LOCK_TIMEOUT = 1

        lock = await lock_manager.lock("resource_name")
        lock_manager._pool.set.assert_called_once_with(
            "resource_name", ANY, 1, True)

        assert lock.resource == "resource_name"
        assert lock.id == ANY
        assert lock.valid is True

    @pytest.mark.asyncio
    async def test_lock_acquired_v2(self):
        with asynctest.mock.patch("aioredis.create_pool") as create_pool:
            create_pool.return_value = mock_redis

            lock_manager = Aioredlock()
            lock_manager.LOCK_TIMEOUT = 1

            lock = await lock_manager.lock("resource_name")
            lock_manager._pool.set.assert_called_once_with(
                "resource_name", ANY, 1, True)

            assert lock.resource == "resource_name"
            assert lock.id == ANY
            assert lock.valid is True

    @pytest.mark.asyncio
    async def test_destroy_lock_manager(self):
        lock_manager = Aioredlock()

        lock_manager._pool = Mock()
        lock_manager._pool.close.return_value = True
        lock_manager._pool.wait_closed = CoroutineMock()

        await lock_manager.destroy()

        lock_manager._pool.close.assert_called_once_with()
        lock_manager._pool.wait_closed.assert_called_once_with()
