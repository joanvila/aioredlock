import pytest
import asynctest

from asynctest import CoroutineMock, patch
from unittest.mock import ANY, call

from aioredlock import Aioredlock
from aioredlock import Lock
from aioredlock.algorithm import validate_lock_timeout


async def dummy_sleep(seconds):
    pass


@pytest.fixture
def locked_lock():
    return Lock("resource_name", 1, True)


@pytest.fixture
def lock_manager_redis_patched():
    with asynctest.patch("aioredlock.algorithm.Redis", CoroutineMock) as mock_redis:
        with patch("asyncio.sleep", dummy_sleep):
            mock_redis.set_lock = CoroutineMock(return_value=(True, 5))
            mock_redis.unset_lock = CoroutineMock(return_value=(True, 5))
            mock_redis.clear_connections = CoroutineMock()

            lock_manager = Aioredlock(lock_timeout=1000, drift=102)

            yield lock_manager, mock_redis


def test_validate_lock_timeout():
    with pytest.raises(ValueError) as exc_info:
        validate_lock_timeout(None, None, -1)
    assert str(exc_info.value) == "Lock timeout must be greater than 0 ms."


class TestAioredlock:

    def test_default_initialization(self):
        with patch("aioredlock.algorithm.Redis.__init__") as mock_redis:
            mock_redis.return_value = None
            lock_manager = Aioredlock()

            mock_redis.assert_called_once_with(
                [{'host': 'localhost', 'port': 6379}],
                lock_manager.lock_timeout
            )
            assert lock_manager.redis
            assert lock_manager.drift == 102

    def test_initialization_with_params(self):
        with patch("aioredlock.algorithm.Redis.__init__") as mock_redis:
            mock_redis.return_value = None
            lock_manager = Aioredlock([{'host': '::1', 'port': 1}])

            mock_redis.assert_called_once_with(
                [{'host': '::1', 'port': 1}],
                lock_manager.lock_timeout
            )
            assert lock_manager.redis
            assert lock_manager.drift == 102

    def test_initialization_with_invalid_timeout(self):
        lock_manager = None
        # Non-positive integers
        with pytest.raises(ValueError):
            lock_manager = Aioredlock(lock_timeout=-1)
        assert lock_manager is None
        # Invalid literal during int() conversion
        with pytest.raises(ValueError):
            lock_manager = Aioredlock(lock_timeout="a")
        assert lock_manager is None

    @pytest.mark.asyncio
    async def test_lock(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched

        lock = await lock_manager.lock('resource')

        redis.set_lock.assert_called_once_with(
            'resource',
            ANY
        )
        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is True

    @pytest.mark.asyncio
    async def test_lock_one_retry(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            (False, 1),
            (True, 1)
        ])

        lock = await lock_manager.lock('resource')

        calls = [
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_not_called()
        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is True

    @pytest.mark.asyncio
    async def test_lock_expire_retries(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            (False, 1),
            (False, 1),
            (False, 1)
        ])

        lock = await lock_manager.lock('resource')

        calls = [
            call('resource', ANY),
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)
        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is False

    @pytest.mark.asyncio
    async def test_lock_one_timeout(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            (True, 1500),
            (True, 1)
        ])

        lock = await lock_manager.lock('resource')

        calls = [
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_not_called()
        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is True

    @pytest.mark.asyncio
    async def test_lock_expire_retries_for_timeouts(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            (True, 1100),
            (True, 1001),
            (True, 2000)
        ])

        lock = await lock_manager.lock('resource')

        calls = [
            call('resource', ANY),
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)
        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is False

    @pytest.mark.asyncio
    async def test_lock_expire_retries_because_drift(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            (True, 898),
            (True, 970),
            (True, 900)
        ])

        lock = await lock_manager.lock('resource')

        calls = [
            call('resource', ANY),
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)
        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is False

    @pytest.mark.asyncio
    async def test_unlock(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched

        await lock_manager.unlock(locked_lock)

        redis.unset_lock.assert_called_once_with(
            locked_lock.resource,
            locked_lock.id
        )
        assert locked_lock.valid is False

    @pytest.mark.asyncio
    async def test_destroy_lock_manager(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched

        await lock_manager.destroy()

        redis.clear_connections.assert_called_once_with()
