import ssl
import uuid
import unittest.mock

import asynctest
import pytest
from asynctest import CoroutineMock, patch

from aioredlock import Aioredlock, Lock


async def dummy_sleep(seconds):
    pass


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


@pytest.fixture
def ssl_context():
    context = ssl.create_default_context()
    with unittest.mock.patch('ssl.create_default_context', return_value=context):
        yield context
