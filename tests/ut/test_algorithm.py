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
    return Lock(None, "resource_name", 1, -1, True)


@pytest.fixture
def lock_manager_redis_patched():
    with asynctest.patch("aioredlock.algorithm.Redis", CoroutineMock) as mock_redis:
        with patch("asyncio.sleep", dummy_sleep):
            mock_redis.set_lock = CoroutineMock(return_value=0.005)
            mock_redis.unset_lock = CoroutineMock(return_value=0.005)
            mock_redis.is_locked = CoroutineMock(return_value=False)
            mock_redis.clear_connections = CoroutineMock()

            lock_manager = Aioredlock()

            yield lock_manager, mock_redis


@pytest.mark.parametrize('method,exc_message', [
    ('_validate_retry_count', "Retry count must be greater or equal 1."),
    ('_validate_retry_delay', "Retry delay must be greater than 0 seconds."),
    ('_validate_internal_lock_timeout', "Internal lock_timeout must be greater than 0 seconds.")
])
def test_validator(method, exc_message):
    with pytest.raises(ValueError) as exc_info:
        getattr(Aioredlock, method)(None, None, -1)
    assert str(exc_info.value) == exc_message


class TestAioredlock:

    def test_default_initialization(self):
        with patch("aioredlock.algorithm.Redis.__init__") as mock_redis:
            mock_redis.return_value = None
            lock_manager = Aioredlock()

            mock_redis.assert_called_once_with(
                [{'host': 'localhost', 'port': 6379}],

            )
            assert lock_manager.redis

    def test_initialization_with_params(self):
        with patch("aioredlock.algorithm.Redis.__init__") as mock_redis:
            mock_redis.return_value = None
            lock_manager = Aioredlock([{'host': '::1', 'port': 1}])

            mock_redis.assert_called_once_with(
                [{'host': '::1', 'port': 1}],
            )
            assert lock_manager.redis

    @pytest.mark.parametrize('param', [
        'retry_count',
        'retry_delay_min',
        'retry_delay_max',
        'internal_lock_timeout'
    ])
    @pytest.mark.parametrize('value,exc_type', [
        (-1, ValueError),
        (0, ValueError),
        ('string', ValueError),
        (None, TypeError)
    ])
    def test_initialization_with_invalid_params(self, param, value, exc_type):
        lock_manager = None
        with pytest.raises(exc_type):
            lock_manager = Aioredlock(**{param: value})
        assert lock_manager is None

    @pytest.mark.asyncio
    async def test_lock(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched

        lock = await lock_manager.lock('resource', 1.0)

        redis.set_lock.assert_called_once_with(
            'resource',
            ANY,
            1.0
        )
        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is True

    @pytest.mark.asyncio
    async def test_lock_with_invalid_param(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched
        with pytest.raises(ValueError):
            await lock_manager.lock("resource", -1)

    @pytest.mark.asyncio
    async def test_lock_one_retry(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            LockError('Can not lock'),
            0.001
        ])

        lock = await lock_manager.lock('resource', 1.0)

        calls = [
            call('resource', ANY, 1.0),
            call('resource', ANY, 1.0)
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
            await lock_manager.lock('resource', 1.0)

        await real_sleep(0.1)  # wait until cleaning is completed

        calls = [
            call('resource', ANY, 1.0),
            call('resource', ANY, 1.0),
            call('resource', ANY, 1.0)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_lock_one_timeout(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched
        redis.set_lock = CoroutineMock(side_effect=[
            1.5,
            0.001
        ])

        lock = await lock_manager.lock('resource', 1.0)

        calls = [
            call('resource', ANY, 1.0),
            call('resource', ANY, 1.0)
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
            1.100,
            1.001,
            2.000
        ])

        with pytest.raises(LockError):
            await lock_manager.lock('resource', 1.0)

        await real_sleep(0.1)  # wait until cleaning is completed

        calls = [
            call('resource', ANY, 1.0),
            call('resource', ANY, 1.0),
            call('resource', ANY, 1.0)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_cancel_lock_(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched

        async def mock_set_lock(*args, **kwargs):
            await real_sleep(1)

        redis.set_lock = CoroutineMock(side_effect=mock_set_lock)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(lock_manager.lock('resource', 1.0), 0.1)

        # The exception handling of the cancelled lock is run in bacround and
        # can not be awaited, so we have to sleep untill the unset_lock has done.
        await real_sleep(0.1)

        redis.set_lock.assert_called_once_with('resource', ANY, 1.0)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_extend_lock(self, lock_manager_redis_patched, locked_lock):
        lock_manager, redis = lock_manager_redis_patched

        lock = await lock_manager.lock('resource', 1.0)
        await lock_manager.extend(lock)

        calls = [
            call('resource', ANY, 1.0),
            call('resource', ANY, 1.0)
        ]
        redis.set_lock.assert_has_calls(calls)

        assert lock.resource == 'resource'
        assert lock.id == ANY
        assert lock.valid is True

        await lock_manager.unlock(lock)
        with pytest.raises(RuntimeError):
            await lock_manager.extend(lock)

    @pytest.mark.asyncio
    async def test_extend_with_invalid_param(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched
        lock = await lock_manager.lock("resource", 1.0)
        with pytest.raises(ValueError):
            await lock_manager.extend(lock, -1)

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
    @pytest.mark.parametrize("by_resource", [True, False])
    @pytest.mark.parametrize("locked", [True, False])
    async def test_is_locked(self, lock_manager_redis_patched, locked_lock, by_resource, locked):
        lock_manager, redis = lock_manager_redis_patched
        redis.is_locked.return_value = locked

        Lock.valid = locked
        resource = locked_lock.resource
        resource_or_lock = resource if by_resource else locked_lock

        res = await lock_manager.is_locked(resource_or_lock)

        assert res == locked
        redis.is_locked.assert_called_once_with(resource)

    @pytest.mark.asyncio
    async def test_is_locked_type_error(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched

        with pytest.raises(TypeError):
            await lock_manager.is_locked(12345)

    @pytest.mark.asyncio
    async def test_context_manager(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched

        async with await lock_manager.lock('resource', 1.0) as lock:
            assert lock.resource == 'resource'
            assert lock.id == ANY
            assert lock.valid is True
            await lock.extend()

        assert lock.valid is False

        calls = [
            call('resource', ANY, 1.0),
            call('resource', ANY, 1.0)
        ]
        redis.set_lock.assert_has_calls(calls)
        redis.unset_lock.assert_called_once_with('resource', ANY)

    @pytest.mark.asyncio
    async def test_destroy_lock_manager(self, lock_manager_redis_patched):
        lock_manager, redis = lock_manager_redis_patched
        lock_manager.unlock = CoroutineMock(side_effect=LockError('Can not lock'))

        await lock_manager.lock("resource", 1.0)
        await lock_manager.destroy()

        redis.clear_connections.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_auto_extend(self):
        with asynctest.patch("aioredlock.algorithm.Redis", CoroutineMock) as mock_redis:
            mock_redis.set_lock = CoroutineMock(return_value=0.005)
            mock_redis.unset_lock = CoroutineMock(return_value=0.005)
            mock_redis.clear_connections = CoroutineMock()

            lock_manager = Aioredlock(internal_lock_timeout=1)
            lock = await lock_manager.lock("resource")

            await real_sleep(lock_manager.internal_lock_timeout * 3)

            calls = [call('resource', lock.id, lock_manager.internal_lock_timeout)
                     for _ in range(5)]
            mock_redis.set_lock.assert_has_calls(calls)

            await lock_manager.destroy()
            mock_redis.clear_connections.assert_called_once_with()
