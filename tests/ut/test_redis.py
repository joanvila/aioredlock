import asyncio
import hashlib
import sys
from distutils.version import StrictVersion
from unittest.mock import MagicMock, call, patch

import aioredis
import pytest

try:
    from aioredis.errors import ReplyError as ResponseError
except ImportError:
    from aioredis.exceptions import ResponseError

from aioredlock.errors import LockError, LockAcquiringError, LockRuntimeError
from aioredlock.redis import Instance, Redis
from aioredlock.sentinel import Sentinel


def callculate_sha1(text):
    sha1 = hashlib.sha1()
    sha1.update(text.encode())
    digest = sha1.hexdigest()
    return digest


EVAL_OK = b'OK'
EVAL_ERROR = ResponseError('ERROR')
CANCELLED = asyncio.CancelledError('CANCELLED')
CONNECT_ERROR = OSError('ERROR')
RANDOM_ERROR = Exception('FAULT')


class FakePool:

    SET_IF_NOT_EXIST = 'SET_IF_NOT_EXIST'

    def __init__(self):

        self.script_cache = {}

        self.evalsha = MagicMock(return_value=asyncio.Future())
        self.evalsha.return_value.set_result(True)
        self.get = MagicMock(return_value=asyncio.Future())
        self.get.return_value.set_result(False)
        self.script_load = MagicMock(side_effect=self._fake_script_load)
        self.execute = MagicMock(side_effect=self._fake_execute)
        self.close = MagicMock(return_value=asyncio.Future())
        self.close.return_value.set_result(True)

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

    async def _fake_script_load(self, script):

        digest = callculate_sha1(script)
        self.script_cache[digest] = script

        return digest.encode()

    async def _fake_execute(self, *args):
        cmd = b' '.join(args[:2])
        if cmd == b'SCRIPT LOAD':
            return await self._fake_script_load(args[-1])


def fake_create_redis_pool(fake_pool):
    """
    Original Redis pool have magick method __await__ to create exclusive
    connection. MagicMock sees this method and thinks that Redis pool
    instance is awaitable and tries to await it.
    To avoit this behavior we are using this constructor with Mock.side_effect
    instead of Mock.return_value.
    """
    async def create_redis_pool(*args, **kwargs):
        return fake_pool
    return create_redis_pool


class TestInstance:

    script_names = ['SET_LOCK_SCRIPT', 'UNSET_LOCK_SCRIPT', 'GET_LOCK_TTL_SCRIPT']

    def test_initialization(self):

        instance = Instance(('localhost', 6379))

        assert instance.connection == ('localhost', 6379)
        assert instance._pool is None
        assert isinstance(instance._lock, asyncio.Lock)

        # scripts
        for name in self.script_names:
            assert getattr(instance, '%s_sha1' % name.lower()) is None

    @pytest.mark.parametrize("connection, address, redis_kwargs", [
        (('localhost', 6379), ('localhost', 6379), {}),
        ({'host': 'localhost', 'port': 6379, 'db': 0, 'password': 'pass'},
            ('localhost', 6379), {'db': 0, 'password': 'pass'}),
        ("redis://host:6379/0?encoding=utf-8",
            "redis://host:6379/0?encoding=utf-8", {})
    ])
    @pytest.mark.asyncio
    async def test_connect_pool_not_created(self, connection, address, redis_kwargs):
        with patch('aioredlock.redis.Instance._create_redis_pool') as \
                create_redis_pool:

            fake_pool = FakePool()
            create_redis_pool.side_effect = fake_create_redis_pool(fake_pool)
            instance = Instance(connection)

            assert instance._pool is None
            pool = await instance.connect()

            create_redis_pool.assert_called_once_with(
                address, **redis_kwargs,
                minsize=1, maxsize=100)
            assert pool is fake_pool
            assert instance._pool is fake_pool

            # scripts
            assert pool.script_load.call_count == len(self.script_names)
            for name in self.script_names:
                digest = getattr(instance, '%s_sha1' % name.lower())
                assert digest
                assert digest in pool.script_cache
            await fake_pool.close()

    @pytest.mark.asyncio
    async def test_connect_pool_not_created_with_minsize_and_maxsize(self):
        connection = {'host': 'localhost', 'port': 6379, 'db': 0, 'password': 'pass', 'minsize': 2, 'maxsize': 5}
        address = ('localhost', 6379)
        redis_kwargs = {'db': 0, 'password': 'pass'}
        with patch('aioredlock.redis.Instance._create_redis_pool') as \
                create_redis_pool:

            fake_pool = FakePool()
            create_redis_pool.side_effect = fake_create_redis_pool(fake_pool)
            instance = Instance(connection)

            assert instance._pool is None
            pool = await instance.connect()

            create_redis_pool.assert_called_once_with(address, **redis_kwargs, minsize=2, maxsize=5)
            assert pool is fake_pool
            assert instance._pool is fake_pool

    @pytest.mark.asyncio
    async def test_connect_pool_already_created(self):

        with patch('aioredlock.redis.Instance._create_redis_pool') as \
                create_redis_pool:
            instance = Instance(('localhost', 6379))
            fake_pool = FakePool()
            instance._pool = fake_pool

            pool = await instance.connect()

            assert not create_redis_pool.called
            assert pool is fake_pool
            assert pool.script_load.called is True

    @pytest.mark.asyncio
    async def test_connect_pool_aioredis_instance(self):

        def awaiter(self):
            yield from []

        pool = FakePool()
        redis_connection = aioredis.Redis(pool)
        instance = Instance(redis_connection)

        assert instance._pool is None
        await instance.connect()
        assert pool.execute.call_count == len(self.script_names)
        assert instance.set_lock_script_sha1 is not None
        assert instance.unset_lock_script_sha1 is not None

    @pytest.mark.asyncio
    async def test_connect_pool_aioredis_instance_with_sentinel(self):

        sentinel = Sentinel(('127.0.0.1', 26379), master='leader')
        pool = FakePool()
        redis_connection = aioredis.Redis(pool)
        with patch.object(sentinel, 'get_master', return_value=asyncio.Future()) as mock_redis:
            if sys.version_info < (3, 8, 0):
                mock_redis.return_value.set_result(redis_connection)
            else:
                mock_redis.return_value = redis_connection
            instance = Instance(sentinel)

            assert instance._pool is None
            await instance.connect()
        assert pool.execute.call_count == len(self.script_names)
        assert instance.set_lock_script_sha1 is not None
        assert instance.unset_lock_script_sha1 is not None

    @pytest.fixture
    def fake_instance(self):
        with patch('aioredlock.redis.Instance._create_redis_pool') as \
                create_redis_pool:
            fake_pool = FakePool()
            create_redis_pool.side_effect = fake_create_redis_pool(fake_pool)
            instance = Instance(('localhost', 6379))
            yield instance

    @pytest.mark.asyncio
    async def test_lock(self, fake_instance):
        instance = fake_instance
        await instance.connect()
        pool = instance._pool

        await instance.set_lock('resource', 'lock_id', 10.0)

        pool.evalsha.assert_called_once_with(
            instance.set_lock_script_sha1,
            keys=['resource'],
            args=['lock_id', 10000]
        )

    @pytest.mark.asyncio
    async def test_get_lock_ttl(self, fake_instance):
        instance = fake_instance
        await instance.connect()
        pool = instance._pool

        await instance.get_lock_ttl('resource', 'lock_id')
        pool.evalsha.assert_called_with(
            instance.get_lock_ttl_script_sha1,
            keys=['resource'],
            args=['lock_id']
        )

    @pytest.mark.asyncio
    async def test_lock_sleep(self, fake_instance, event_loop):
        instance = fake_instance

        async def hold_lock(instance):
            async with instance._lock:
                await asyncio.sleep(.1)
                instance._pool = FakePool()

        event_loop.create_task(hold_lock(instance))
        await asyncio.sleep(.1)
        await instance.connect()
        pool = instance._pool

        await instance.set_lock('resource', 'lock_id', 10.0)

        pool.evalsha.assert_called_once_with(
            instance.set_lock_script_sha1,
            keys=['resource'],
            args=['lock_id', 10000]
        )

        instance._pool = None
        await instance.close()
        assert pool.close.called is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'func,args,expected_keys,expected_args',
        (
            ('set_lock', ('resource', 'lock_id', 10.0), ['resource'], ['lock_id', 10000]),
            ('unset_lock', ('resource', 'lock_id'), ['resource'], ['lock_id']),
            ('get_lock_ttl', ('resource', 'lock_id'), ['resource'], ['lock_id']),
        )
    )
    async def test_lock_without_scripts(self, fake_coro, fake_instance, func, args, expected_keys, expected_args):
        instance = fake_instance
        await instance.connect()
        pool = instance._pool
        pool.evalsha.side_effect = [aioredis.errors.ReplyError('NOSCRIPT'), fake_coro(True)]

        await getattr(instance, func)(*args)

        assert pool.evalsha.call_count == 2
        assert pool.script_load.call_count == 6  # for 3 scripts.

        pool.evalsha.assert_called_with(
            getattr(instance, '{0}_script_sha1'.format(func)),
            keys=expected_keys,
            args=expected_args,
        )

    @pytest.mark.asyncio
    async def test_unset_lock(self, fake_instance):
        instance = fake_instance
        await instance.connect()
        pool = instance._pool

        await instance.unset_lock('resource', 'lock_id')

        pool.evalsha.assert_called_once_with(
            instance.unset_lock_script_sha1,
            keys=['resource'],
            args=['lock_id']
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("get_return_value,locked", [
        (b'lock_identifier', True),
        (None, False),
    ])
    async def test_is_locked(self, fake_instance, get_return_value, locked):
        instance = fake_instance
        await instance.connect()
        pool = instance._pool

        pool.get.return_value = asyncio.Future()
        pool.get.return_value.set_result(get_return_value)

        res = await instance.is_locked('resource')

        assert res == locked
        pool.get.assert_called_once_with('resource')


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
    redis = Redis(redis_two_connections)

    for instance in redis.instances:
        instance._pool = pool

    yield redis, pool


@pytest.fixture
def mock_redis_three_instances(redis_three_connections):
    pool = FakePool()
    redis = Redis(redis_three_connections)

    for instance in redis.instances:
        instance._pool = pool

    yield redis, pool


class TestRedis:

    def test_initialization(self, redis_two_connections):
        with patch("aioredlock.redis.Instance.__init__") as mock_instance:
            mock_instance.return_value = None

            redis = Redis(redis_two_connections)

            calls = [
                call({'host': 'localhost', 'port': 6379}),
                call({'host': '127.0.0.1', 'port': 6378})
            ]
            mock_instance.assert_has_calls(calls)
            assert len(redis.instances) == 2

    parametrize_methods = pytest.mark.parametrize("method_name, call_args", [
        ('set_lock', {'keys': ['resource'], 'args':['lock_id', 10000]}),
        ('unset_lock', {'keys': ['resource'], 'args':['lock_id']}),
        ('get_lock_ttl', {'keys': ['resource'], 'args':['lock_id']}),
    ])

    def _setup_evalsha_call_args(self, digest, keys, args):
        if StrictVersion(aioredis.__version__) >= StrictVersion('2.0.0'):
            return call(
                digest,
                len(keys),
                *keys,
                *args,
            )
        else:
            return call(
                digest=digest,
                keys=keys,
                args=args,
            )

    @pytest.mark.asyncio
    @parametrize_methods
    async def test_lock(
            self, mock_redis_two_instances,
            method_name, call_args
    ):
        redis, pool = mock_redis_two_instances

        method = getattr(redis, method_name)

        await method('resource', 'lock_id')

        script_sha1 = getattr(redis.instances[0], '%s_script_sha1' % method_name)

        calls = [self._setup_evalsha_call_args(script_sha1, **call_args)] * 2
        pool.evalsha.assert_has_calls(calls)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("get_return_value,locked", [
        (b'lock_identifier', True),
        (None, False),
    ])
    async def test_is_locked(self, mock_redis_two_instances, get_return_value, locked):
        redis, pool = mock_redis_two_instances

        pool.get.return_value = asyncio.Future()
        pool.get.return_value.set_result(get_return_value)

        res = await redis.is_locked('resource')

        calls = [call('resource')] * 2
        pool.get.assert_has_calls(calls)
        assert res == locked

    @pytest.mark.asyncio
    @parametrize_methods
    async def test_lock_one_of_two_instances_failed(
            self, fake_coro, mock_redis_two_instances,
            method_name, call_args
    ):
        redis, pool = mock_redis_two_instances
        pool.evalsha = MagicMock(side_effect=[EVAL_ERROR, EVAL_OK])

        method = getattr(redis, method_name)

        with pytest.raises(LockError):
            await method('resource', 'lock_id')

        script_sha1 = getattr(redis.instances[0], '%s_script_sha1' % method_name)

        calls = [self._setup_evalsha_call_args(script_sha1, **call_args)] * 2
        pool.evalsha.assert_has_calls(calls)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("redis_result, success", [
        ([EVAL_OK, EVAL_OK, EVAL_OK], True),
        ([EVAL_OK, EVAL_OK, EVAL_ERROR], True),
        ([EVAL_OK, EVAL_ERROR, CONNECT_ERROR], False),
        ([EVAL_ERROR, EVAL_ERROR, CONNECT_ERROR], False),
        ([EVAL_ERROR, CONNECT_ERROR, RANDOM_ERROR], False),
        ([CANCELLED, CANCELLED, CANCELLED], False),
    ])
    @parametrize_methods
    async def test_three_instances_combination(
            self,
            fake_coro,
            mock_redis_three_instances,
            redis_result,
            success,
            method_name, call_args,
    ):
        redis, pool = mock_redis_three_instances
        redis_result = [fake_coro(result) if isinstance(result, bytes) else result for result in redis_result]
        pool.evalsha = MagicMock(side_effect=redis_result)

        method = getattr(redis, method_name)

        if success:
            await method('resource', 'lock_id')
        else:
            with pytest.raises(LockError) as exc_info:
                await method('resource', 'lock_id')
            assert hasattr(exc_info.value, '__cause__')
            assert isinstance(exc_info.value.__cause__, BaseException)

        script_sha1 = getattr(redis.instances[0],
                              '%s_script_sha1' % method_name)

        calls = [self._setup_evalsha_call_args(script_sha1, **call_args)] * 3
        pool.evalsha.assert_has_calls(calls)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("redis_result, error", [
        ([EVAL_OK, EVAL_ERROR, CONNECT_ERROR], LockRuntimeError),
        ([EVAL_ERROR, EVAL_ERROR, CONNECT_ERROR], LockRuntimeError),
        ([EVAL_ERROR, CONNECT_ERROR, RANDOM_ERROR], LockRuntimeError),
        ([EVAL_ERROR, EVAL_ERROR, EVAL_OK], LockAcquiringError),
        ([CANCELLED, CANCELLED, CANCELLED], LockError),
        ([RANDOM_ERROR, CANCELLED, CANCELLED], LockError),
    ])
    @parametrize_methods
    async def test_three_instances_combination_errors(
            self,
            fake_coro,
            mock_redis_three_instances,
            redis_result,
            error,
            method_name, call_args,
    ):
        redis, pool = mock_redis_three_instances
        redis_result = [fake_coro(result) if isinstance(result, bytes) else result for result in redis_result]
        pool.evalsha = MagicMock(side_effect=redis_result)

        method = getattr(redis, method_name)

        with pytest.raises(error) as exc_info:
            await method('resource', 'lock_id')

        assert hasattr(exc_info.value, '__cause__')
        assert isinstance(exc_info.value.__cause__, BaseException)

        script_sha1 = getattr(redis.instances[0],
                              '%s_script_sha1' % method_name)

        calls = [self._setup_evalsha_call_args(script_sha1, **call_args)] * 3
        pool.evalsha.assert_has_calls(calls)

    @pytest.mark.asyncio
    async def test_clear_connections(self, mock_redis_two_instances):
        redis, pool = mock_redis_two_instances
        pool.close = MagicMock()
        pool.wait_closed = MagicMock(return_value=asyncio.Future())
        pool.wait_closed.return_value.set_result(True)

        await redis.clear_connections()

        pool.close.assert_has_calls([call(), call()])
        pool.wait_closed.assert_has_calls([call(), call()])

        pool.close = MagicMock()
        pool.wait_closed = MagicMock(return_value=asyncio.Future())

        await redis.clear_connections()

        assert pool.close.called is False

    @pytest.mark.asyncio
    async def test_get_lock(self, mock_redis_two_instances, ):
        redis, pool = mock_redis_two_instances

        await redis.get_lock_ttl('resource', 'lock_id')

        script_sha1 = getattr(redis.instances[0], 'get_lock_ttl_script_sha1')

        calls = [self._setup_evalsha_call_args(script_sha1, keys=['resource'], args=['lock_id'])]
        pool.evalsha.assert_has_calls(calls)
        # assert 0
