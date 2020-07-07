import uuid

import asynctest
import pytest
from asynctest import CoroutineMock, patch

from aioredlock import Aioredlock, Lock


async def dummy_sleep(seconds):
    pass


@pytest.fixture
def locked_lock(lock_manager_redis_patched):
    lock_manager, _ = lock_manager_redis_patched
    lock = Lock(lock_manager, "resource_name", 1, -1, True)
    lock_manager._locks[lock.resource] = lock
    return lock


@pytest.fixture
def unlocked_lock(lock_manager_redis_patched):
    lock_manager, _ = lock_manager_redis_patched
    lock = Lock(lock_manager, "other_resource_name", 1, -1, False)
    lock_manager._locks[lock.resource] = lock
    return lock


@pytest.fixture
def lock_manager_redis_patched():
    with asynctest.patch("aioredlock.algorithm.Redis", CoroutineMock) as mock_redis:
        with patch("asyncio.sleep", dummy_sleep):
            mock_redis.set_lock = CoroutineMock(return_value=0.005)
            mock_redis.unset_lock = CoroutineMock(return_value=0.005)
            mock_redis.is_locked = CoroutineMock(return_value=False)
            mock_redis.clear_connections = CoroutineMock()

            lock_manager = Aioredlock(internal_lock_timeout=1.0)

            yield lock_manager, mock_redis


@pytest.fixture
def aioredlock_patched():
    with asynctest.patch("aioredlock.algorithm.Aioredlock", CoroutineMock) \
            as mock_aioredlock:
        with patch("asyncio.sleep", dummy_sleep):

            async def dummy_lock(resource):
                lock_identifier = str(uuid.uuid4())
                return Lock(mock_aioredlock, resource,
                            lock_identifier, valid=True)

            mock_aioredlock.lock = CoroutineMock(side_effect=dummy_lock)
            mock_aioredlock.extend = CoroutineMock()
            mock_aioredlock.unlock = CoroutineMock()
            mock_aioredlock.destroy = CoroutineMock()

            yield mock_aioredlock
