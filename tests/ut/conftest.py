import asyncio
import ssl
import uuid
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

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
    with patch("aioredlock.algorithm.Redis") as mock_redis, \
            patch("asyncio.sleep", dummy_sleep):
        mock_redis.set_lock.return_value = asyncio.Future()
        mock_redis.set_lock.return_value.set_result(0.005)
        mock_redis.unset_lock.return_value = asyncio.Future()
        mock_redis.unset_lock.return_value.set_result(0.005)
        mock_redis.is_locked.return_value = asyncio.Future()
        mock_redis.is_locked.return_value.set_result(False)
        mock_redis.clear_connections.return_value = asyncio.Future()
        mock_redis.clear_connections.return_value.set_result(MagicMock())
        mock_redis.get_lock_ttl.return_value = asyncio.Future()
        mock_redis.get_lock_ttl.return_value.set_result(Lock(None, "resource_name", 1, -1, True))

        lock_manager = Aioredlock(internal_lock_timeout=1.0)
        lock_manager.redis = mock_redis

        yield lock_manager, mock_redis


@pytest.fixture
def aioredlock_patched():
    with patch("aioredlock.algorithm.Aioredlock", MagicMock) as mock_aioredlock, \
            patch("asyncio.sleep", dummy_sleep):

        async def dummy_lock(resource):
            lock_identifier = str(uuid.uuid4())
            return Lock(mock_aioredlock, resource,
                        lock_identifier, valid=True)

        mock_aioredlock.lock = MagicMock(side_effect=dummy_lock)
        mock_aioredlock.extend = MagicMock(return_value=asyncio.Future())
        mock_aioredlock.extend.return_value.set_result(MagicMock())
        mock_aioredlock.unlock = MagicMock(return_value=asyncio.Future())
        mock_aioredlock.unlock.return_value.set_result(MagicMock())
        # mock_aioredlock.get_lock_ttl = MagicMock(return_value=asyncio.Future())
        # mock_aioredlock.get_lock_ttl.return_value.set_result(MagicMock())

        yield mock_aioredlock


@pytest.fixture
def ssl_context():
    context = ssl.create_default_context()
    with patch('ssl.create_default_context', return_value=context):
        yield context


@pytest.fixture
def fake_coro():
    async def func(thing):
        return thing
    return func
