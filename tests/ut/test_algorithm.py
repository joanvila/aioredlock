import pytest

from asynctest import CoroutineMock, patch
from unittest.mock import Mock, ANY

from aioredlock import Aioredlock
from aioredlock import Lock


class FakePool:

    SET_IF_NOT_EXIST = 'SET_IF_NOT_EXIST'

    def __init__(self):
        self.set = CoroutineMock(return_value=True)
        self.eval = CoroutineMock()

    def __await__(self):
        yield
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def __call__(self):
        return self


@pytest.fixture
def lock_manager():
    lock_manager = Aioredlock()
    pool = FakePool()
    lock_manager._connect = pool
    lock_manager.LOCK_TIMEOUT = 1
    yield lock_manager, pool


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
    async def test_connect_pool_not_created(self):
        with patch("aioredis.create_pool") as create_pool:
            fake_pool = FakePool()
            create_pool.return_value = fake_pool
            lock_manager = Aioredlock()

            pool = await lock_manager._connect()

            create_pool.assert_called_once_with(('localhost', 6379), minsize=5)
            assert pool is fake_pool

    @pytest.mark.asyncio
    async def test_connect_pool_already_created(self):
        with patch("aioredis.create_pool") as create_pool:
            lock_manager = Aioredlock()
            fake_pool = FakePool()
            lock_manager._pool = fake_pool

            pool = await lock_manager._connect()

            assert not create_pool.called
            assert pool is fake_pool

    @pytest.mark.asyncio
    async def test_destroy_lock_manager(self):
        lock_manager = Aioredlock()

        lock_manager._pool = Mock()
        lock_manager._pool.close.return_value = True
        lock_manager._pool.wait_closed = CoroutineMock()

        await lock_manager.destroy()

        lock_manager._pool.close.assert_called_once_with()
        lock_manager._pool.wait_closed.assert_called_once_with()
