import asyncio
import contextlib
import ssl
import sys
from unittest import mock

import aioredlock.sentinel
from aioredlock.sentinel import Sentinel
from aioredlock.sentinel import SentinelConfigError

import pytest

pytestmark = [pytest.mark.asyncio]


@contextlib.contextmanager
def mock_aioredis_sentinel():
    if sys.version_info < (3, 8, 0):
        mock_obj = mock.MagicMock()
        mock_obj.master_for.return_value = asyncio.Future()
        mock_obj.master_for.return_value.set_result(True)
    else:
        mock_obj = mock.AsyncMock()
        mock_obj.master_for.return_value = True
    with mock.patch.object(aioredlock.sentinel, 'create_sentinel') as mock_sentinel:
        if sys.version_info < (3, 8, 0):
            mock_sentinel.return_value = asyncio.Future()
            mock_sentinel.return_value.set_result(mock_obj)
        else:
            mock_sentinel.return_value = mock_obj
        yield mock_sentinel


@pytest.mark.parametrize(
    'connection,kwargs,expected_kwargs,expected_master,with_ssl', (
        (
            {'host': '127.0.0.1', 'port': 26379, 'master': 'leader'},
            {},
            {'sentinels': [('127.0.0.1', 26379)], 'minsize': 1, 'maxsize': 100},
            'leader',
            {},
        ),
        (
            'redis://:password@localhost:12345/0?master=whatever&encoding=utf-8&minsize=2&maxsize=5',
            {},
            {
                'sentinels': [('localhost', 12345)],
                'db': 0,
                'encoding': 'utf-8',
                'password': 'password',
                'minsize': 2,
                'maxsize': 5,
            },
            'whatever',
            {},
        ),
        (
            'redis://:password@localhost:12345/0?master=whatever&encoding=utf-8',
            {'master': 'everything', 'password': 'newpass', 'db': 3},
            {
                'sentinels': [('localhost', 12345)],
                'db': 3,
                'encoding': 'utf-8',
                'password': 'newpass',
                'minsize': 1,
                'maxsize': 100,
            },
            'everything',
            {},
        ),
        (
            'rediss://:password@localhost:12345/2?master=whatever&encoding=utf-8',
            {},
            {
                'sentinels': [('localhost', 12345)],
                'db': 2,
                'encoding': 'utf-8',
                'password': 'password',
                'minsize': 1,
                'maxsize': 100,
            },
            'whatever',
            {'verify_mode': ssl.CERT_REQUIRED, 'check_hostname': True},
        ),
        (
            'rediss://:password@localhost:12345/2?master=whatever&encoding=utf-8&ssl_cert_reqs=CERT_NONE',
            {},
            {
                'sentinels': [('localhost', 12345)],
                'db': 2,
                'encoding': 'utf-8',
                'password': 'password',
                'minsize': 1,
                'maxsize': 100,
            },
            'whatever',
            {'verify_mode': ssl.CERT_NONE, 'check_hostname': False},
        ),
        (
            'rediss://localhost:12345/2?master=whatever&encoding=utf-8&ssl_cert_reqs=CERT_OPTIONAL',
            {},
            {
                'sentinels': [('localhost', 12345)],
                'db': 2,
                'encoding': 'utf-8',
                'password': None,
                'minsize': 1,
                'maxsize': 100,
            },
            'whatever',
            {'verify_mode': ssl.CERT_OPTIONAL, 'check_hostname': True},
        ),
        (
            ('127.0.0.1', 1234),
            {'master': 'blah', 'ssl_context': True},
            {
                'sentinels': [('127.0.0.1', 1234)],
                'minsize': 1,
                'maxsize': 100,
            },
            'blah',
            {},
        ),
        (
            [('127.0.0.1', 1234), ('blah', 4829)],
            {'master': 'blah', 'ssl_context': False},
            {
                'sentinels': [('127.0.0.1', 1234), ('blah', 4829)],
                'minsize': 1,
                'maxsize': 100,
                'ssl': False,
            },
            'blah',
            {},
        ),
    )
)
async def test_sentinel(ssl_context, connection, kwargs, expected_kwargs, expected_master, with_ssl):
    with mock_aioredis_sentinel() as mock_sentinel:
        sentinel = Sentinel(connection, **kwargs)
        assert await sentinel.get_master()
    assert mock_sentinel.called
    if with_ssl or kwargs.get('ssl_context') is True:
        expected_kwargs['ssl'] = ssl_context
    mock_sentinel.assert_called_with(**expected_kwargs)
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with(expected_master)
    if with_ssl:
        assert ssl_context.check_hostname is with_ssl['check_hostname']
        assert ssl_context.verify_mode is with_ssl['verify_mode']


@pytest.mark.parametrize(
    'connection',
    (
        'redis://localhost:1234/0',
        'redis://localhost:1234/blah',
        object(),
    )
)
async def test_sentinel_config_errors(connection):
    with pytest.raises(SentinelConfigError):
        Sentinel(connection)
