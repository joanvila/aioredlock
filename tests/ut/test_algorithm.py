import asyncio
from unittest.mock import ANY, call

import asynctest
import pytest
from asynctest import CoroutineMock, patch

from aioredlock import Aioredlock, Lock, LockError


async def dummy_sleep(seconds):
    pass

real_sleep = asyncio.sleep


@pytest.fixture
def locked_lock():
    return Lock("resource", ANY, True)


@pytest.fixture
def lock_manager_redis_patched():
    with asynctest.patch("aioredlock.algorithm.Redis", CoroutineMock) as mock_redis:
        with patch("asyncio.sleep", dummy_sleep):
            mock_redis.set_lock = CoroutineMock(return_value=5)
            mock_redis.unset_lock = CoroutineMock(return_value=5)
            mock_redis.clear_connections = CoroutineMock()

            lock_manager = Aioredlock()
            lock_manager.LOCK_TIMEOUT = 1000
            lock_manager.retry_count = 3
            lock_manager.drift = 102

            yield lock_manager, mock_redis


class TestAioredlock:

    def test_default_initialization(self):
        with patch("aioredlock.algorithm.Redis.__init__") as mock_redis:
            mock_redis.return_value = None
            lock_manager = Aioredlock()

            mock_redis.assert_called_once_with(
                [{'host': 'localhost', 'port': 6379}],
                lock_manager.LOCK_TIMEOUT
            )
            assert lock_manager.redis
            assert lock_manager.drift == 102

    def test_initialization_with_params(self):
        with patch("aioredlock.algorithm.Redis.__init__") as mock_redis:
            mock_redis.return_value = None
            lock_manager = Aioredlock([{'host': '::1', 'port': 1}])

            mock_redis.assert_called_once_with(
                [{'host': '::1', 'port': 1}],
                lock_manager.LOCK_TIMEOUT
            )
            assert lock_manager.redis
            assert lock_manager.drift == 102

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
            LockError('Can not lock'),
            1
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
            LockError('Can not lock'),
            LockError('Can not lock'),
            LockError('Can not lock')
        ])

        with pytest.raises(LockError):
            await lock_manager.lock('resource')

        await real_sleep(0.1)  # wait until cleaning is completed

        calls = [
            call('resource', ANY),
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_lock_one_timeout(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            1500,
            1
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
            1100,
            1001,
            2000
        ])

        with pytest.raises(LockError):
            await lock_manager.lock('resource')

        await real_sleep(0.1)  # wait until cleaning is completed

        calls = [
            call('resource', ANY),
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_lock_expire_retries_because_drift(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            898,
            970,
            900
        ])

        with pytest.raises(LockError):
            await lock_manager.lock('resource')

        await real_sleep(0.1)  # wait until cleaning is completed

        calls = [
            call('resource', ANY),
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_cancel_lock_(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched

        async def mock_set_lock(*args, **kwargs):
            await real_sleep(1)
            return 1000

        redis.set_lock = CoroutineMock(side_effect=mock_set_lock)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(lock_manager.lock('resource'), 0.1)

        await real_sleep(0.1)
        # Exception handling of cancelled lock runs in bacround
        # and can not be awaited, so we have to wait untill unset_lock is
        # complited with this asincio.sleep()

        redis.set_lock.assert_called_once_with('resource', ANY)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_extend_lock(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched

        lock = await lock_manager.lock('resource')
        await lock_manager.extend(lock)

        calls = [
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)

        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is True

        await lock_manager.unlock(lock)
        with pytest.raises(RuntimeError):
            await lock_manager.extend(lock)

    @pytest.mark.asyncio
    async def test_unlock(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched

        await lock_manager.unlock(locked_lock)

        redis.unset_lock.assert_called_once_with('resource', ANY)
        assert locked_lock.valid is False

    @pytest.mark.asyncio
    async def test_context_manager(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched

        async with await lock_manager.lock('resource') as lock:
            assert lock.resource == 'resource'
            assert lock.id == ANY
            assert lock.valid is True
            await lock.extend()

        assert lock.valid is False

        calls = [
            call('resource', ANY),
            call('resource', ANY)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_destroy_lock_manager(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched

        await lock_manager.destroy()

        redis.clear_connections.assert_called_once_with()
