import asyncio
from unittest.mock import MagicMock, call

import pytest
from asynctest import CoroutineMock, patch

from aioredlock.redis import Instance, Redis


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

    def is_fake(self):
        # Only for development purposes
        return True


class TestInstance:

    def test_initialization(self):
        instance = Instance('localhost', 6379)
        assert instance.host == 'localhost'
        assert instance.port == 6379
        assert instance._pool is None
        assert isinstance(instance._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_connect_pool_not_created(self):
        with patch('aioredlock.redis.Instance._create_redis_pool') as \
                create_redis_pool:
            fake_pool = FakePool()
            create_redis_pool.return_value = fake_pool
            instance = Instance('localhost', 6379)

            assert instance._pool is None
            pool = await instance.connect()

            create_redis_pool.assert_called_once_with(
                ('localhost', 6379),
                db=0, password=None,
                minsize=1, maxsize=100)
            assert pool is fake_pool
            assert instance._pool is fake_pool

    @pytest.mark.asyncio
    async def test_connect_pool_already_created(self):

        with patch('aioredlock.redis.Instance._create_redis_pool') as \
                create_redis_pool:
            instance = Instance('localhost', 6379)
            fake_pool = FakePool()
            instance._pool = fake_pool

            pool = await instance.connect()

            assert not create_redis_pool.called
            assert pool is fake_pool


@pytest.fixture
def redis_two_connections():
    return [
        {'host': 'localhost', 'port': 6379},
        {'host': '127.0.0.1', 'port': 6378}
    ]


@pytest.fixture
def redis_three_connections():
    return [
        {'host': 'localhost', 'port': 6379},
        {'host': '127.0.0.1', 'port': 6378},
        {'host': '8.8.8.8', 'port': 6377}
    ]


@pytest.fixture
def mock_redis_two_instances(redis_two_connections):
    pool = FakePool()
    redis = Redis(redis_two_connections, 10)

    for instance in redis.instances:
        instance._pool = pool

    yield redis, pool


@pytest.fixture
def mock_redis_three_instances(redis_three_connections):
    pool = FakePool()
    redis = Redis(redis_three_connections, 10)

    for instance in redis.instances:
        instance._pool = pool

    yield redis, pool


class TestRedis:

    def test_initialization(self, redis_two_connections):
        with patch("aioredlock.redis.Instance.__init__") as mock_instance:
            mock_instance.return_value = None

            redis = Redis(redis_two_connections, 10)

            calls = [
                call(host='localhost', port=6379),
                call(host='127.0.0.1', port=6378)
            ]
            mock_instance.assert_has_calls(calls)
            assert len(redis.instances) == 2
            assert redis.lock_timeout == 10

    @pytest.mark.asyncio
    async def test_set_lock(self, mock_redis_two_instances):
        redis, pool = mock_redis_two_instances

        locked, elapsed_time = await redis.set_lock('resource', 'lock_id')

        calls = [
            call('resource', 'lock_id', pexpire=10,
                 exist=pool.SET_IF_NOT_EXIST),
            call('resource', 'lock_id', pexpire=10, exist=pool.SET_IF_NOT_EXIST)
        ]
        pool.set.assert_has_calls(calls)
        assert locked is True

    @pytest.mark.asyncio
    async def test_set_lock_one_of_two_instances_failed(self, mock_redis_two_instances):
        redis, pool = mock_redis_two_instances
        pool.set = CoroutineMock(side_effect=[False, True])

        locked, elapsed_time = await redis.set_lock('resource', 'lock_id')

        calls = [
            call('resource', 'lock_id', pexpire=10,
                 exist=pool.SET_IF_NOT_EXIST),
            call('resource', 'lock_id', pexpire=10, exist=pool.SET_IF_NOT_EXIST)
        ]
        pool.set.assert_has_calls(calls)
        assert locked is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("redis_failures, lock_acquired", [
        ([True, True, True], True),
        ([True, True, False], True),
        ([True, False, False], False),
        ([False, False, False], False),
    ])
    async def test_set_three_instances_combination(
            self,
            mock_redis_three_instances,
            redis_failures,
            lock_acquired):
        redis, pool = mock_redis_three_instances
        pool.set = CoroutineMock(side_effect=redis_failures)

        locked, elapsed_time = await redis.set_lock('resource', 'lock_id')

        calls = [
            call('resource', 'lock_id', pexpire=10,
                 exist=pool.SET_IF_NOT_EXIST),
            call('resource', 'lock_id', pexpire=10,
                 exist=pool.SET_IF_NOT_EXIST),
            call('resource', 'lock_id', pexpire=10, exist=pool.SET_IF_NOT_EXIST)
        ]
        pool.set.assert_has_calls(calls)
        assert locked is lock_acquired

    @pytest.mark.asyncio
    async def test_run_lua(self, mock_redis_two_instances):
        redis, pool = mock_redis_two_instances
        keys = ['k1', 'k2']
        args = ['a1', 'a2']

        await redis.run_lua('script', keys=keys, args=args)

        calls = [
            call('script', keys=keys, args=args),
            call('script', keys=keys, args=args)
        ]
        pool.eval.assert_has_calls(calls)

    @pytest.mark.asyncio
    async def test_clear_connections(self, mock_redis_two_instances):
        redis, pool = mock_redis_two_instances
        pool.close = MagicMock()
        pool.wait_closed = CoroutineMock()

        await redis.clear_connections()

        pool.close.assert_has_calls([call(), call()])
        pool.wait_closed.assert_has_calls([call(), call()])
